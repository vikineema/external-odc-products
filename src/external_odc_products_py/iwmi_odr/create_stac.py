"""
Create per dataset metadata (stac files) for the IWMI ODR
products.

Datasource:
"""

import json
import logging
import os
import sys
from pathlib import Path

import click
import numpy as np
from eodatasets3.serialise import to_path  # noqa F401
from eodatasets3.stac import to_stac_item
from odc.aws import s3_dump

from external_odc_products_py.io import (
    check_directory_exists,
    check_file_exists,
    find_geotiff_files,
    get_filesystem,
    is_gcsfs_path,
    is_s3_path,
    is_url,
)
from external_odc_products_py.iwmi_odr.prepare_metadata import prepare_dataset
from external_odc_products_py.logs import get_logger
from external_odc_products_py.utils import crs_str_to_int, download_product_yaml

log = get_logger(Path(__file__).stem, level=logging.INFO)


def fix_proj_code_property(stac_file: dict) -> dict:
    """
    Implement fix for proj code property.

    Parameters
    ----------
    stac_file : dict
        Stac item from converting a dataset doc to stac using
        `eodatasets3.stac.to_stac_item`

    Returns
    -------
    dict
        Updated stac_item
    """
    # Fix proj:code property in properties
    properties = stac_file["properties"]
    proj_code = properties.get("proj:code")

    if proj_code:
        new_properties = {}
        for k, v in properties.items():
            if k == "proj:code":
                new_properties["proj:epsg"] = crs_str_to_int(proj_code)
            else:
                new_properties[k] = v
    else:
        new_properties = None

    # Update properties
    if new_properties:
        stac_file["properties"] = new_properties

    # Fix proj:code property in assets
    assets = stac_file["assets"]
    for measurement in assets.keys():
        measurement_properties = assets[measurement]
        proj_code = measurement_properties.get("proj:code")
        if proj_code:
            new_measurement_properties = {}
            for k, v in measurement_properties.items():
                if k == "proj:code":
                    new_measurement_properties["proj:epsg"] = crs_str_to_int(proj_code)
                else:
                    new_measurement_properties[k] = v
        else:
            new_measurement_properties = None

        # Update property in assets
        if new_measurement_properties:
            stac_file["assets"][measurement] = new_measurement_properties

    return stac_file


@click.command(
    "create-stac-files",
    help="Create per dataset metadata (stac files) for IWMI ODR products.",
    no_args_is_help=True,
)
@click.option(
    "--product-name",
    type=str,
    help="Name of the product to generate the stac item files for",
)
@click.option(
    "--product-yaml", type=str, help="File path or URL to the product definition yaml file"
)
@click.option(
    "--geotiffs-dir",
    type=str,
    help="File path to the directory containing the COG files",
)
@click.option(
    "--stac-output-dir",
    type=str,
    help="Directory to write the stac files docs to",
)
@click.option("--overwrite/--no-overwrite", default=False)
@click.option(
    "--max-parallel-steps",
    default=1,
    type=int,
    help="Maximum number of parallel steps/pods to have in the workflow.",
)
@click.option(
    "--worker-idx",
    default=0,
    type=int,
    help="Sequential index which will be used to define the range of geotiffs the pod will work with.",
)
def create_stac_files(
    product_name: str,
    product_yaml: str,
    geotiffs_dir: str,
    stac_output_dir: str,
    overwrite: bool,
    max_parallel_steps: int,
    worker_idx: int,
):

    # Validate products
    valid_product_names = [
        "iwmi_blue_et_monthly",
        "iwmi_green_et_monthly",
    ]
    if product_name not in valid_product_names:
        raise NotImplementedError(
            f"Stac file generation has not been implemented for {product_name}"
        )

    # Set to temporary dir as output metadata yaml files are not required.
    metadata_output_dir = "tmp/metadata_docs"
    if product_name not in os.path.basename(metadata_output_dir.rstrip("/")):
        metadata_output_dir = os.path.join(metadata_output_dir, product_name)

    if is_s3_path(metadata_output_dir):
        raise RuntimeError("Metadata files require to be written to a local directory")

    # Path to product yaml
    if not is_s3_path(product_yaml):
        if is_url(product_yaml):
            product_yaml = download_product_yaml(product_yaml)
    else:
        NotImplemented("Product yaml is expected to be a local file or url not s3 path")

    # Directory to write the stac files to
    if product_name not in os.path.basename(stac_output_dir.rstrip("/")):
        stac_output_dir = os.path.join(stac_output_dir, product_name)

    # Find all the geotiffs files in the directory
    all_geotiff_files = find_geotiff_files(geotiffs_dir)
    log.info(f"Found {len(all_geotiff_files)} geotiffs in {geotiffs_dir}")

    # Split files equally among the workers
    task_chunks = np.array_split(np.array(all_geotiff_files), max_parallel_steps)
    task_chunks = [chunk.tolist() for chunk in task_chunks]
    task_chunks = list(filter(None, task_chunks))

    # In case of the index being bigger than the number of positions in the array, the extra POD isn't necessary
    if len(task_chunks) <= worker_idx:
        log.warning(f"Worker {worker_idx} Skipped!")
        sys.exit(0)

    log.info(f"Executing worker {worker_idx}")

    geotiffs = task_chunks[worker_idx]

    log.info(f"Generating stac files for the product {product_name}")

    for idx, geotiff in enumerate(geotiffs):
        log.info(f"Generating stac file for {geotiff} {idx+1}/{len(geotiffs)}")

        # File system Path() to the dataset
        # or gsutil URI prefix  (gs://bucket/key) to the dataset.
        if not is_s3_path(geotiff) and not is_gcsfs_path(geotiff):
            dataset_path = Path(geotiff).resolve()
        else:
            dataset_path = geotiff

        tile_id = os.path.basename(dataset_path).removesuffix(".tif")

        metadata_output_path = Path(
            os.path.join(metadata_output_dir, f"{tile_id}.odc-metadata.yaml")
        ).resolve()
        stac_item_destination_url = os.path.join(stac_output_dir, f"{tile_id}.stac-item.json")

        # Check if the stac item exist:
        if not overwrite:
            if check_file_exists(stac_item_destination_url):
                log.info(
                    f"{stac_item_destination_url} exists! Skipping stac file generation for {dataset_path}"
                )
                continue

        # Create the required parent directories
        metadata_output_parent_dir = os.path.dirname(metadata_output_path)
        if not check_directory_exists(metadata_output_parent_dir):
            fs = get_filesystem(metadata_output_parent_dir, anon=False)
            fs.makedirs(metadata_output_parent_dir, exist_ok=True)
            log.info(f"Created the directory {metadata_output_parent_dir}")

        stac_item_parent_dir = os.path.dirname(stac_item_destination_url)
        if not check_directory_exists(stac_item_parent_dir):
            fs = get_filesystem(stac_item_parent_dir, anon=False)
            fs.makedirs(stac_item_parent_dir, exist_ok=True)
            log.info(f"Created the directory {stac_item_parent_dir}")

        # Prepare the dataset's metadata doc
        dataset_doc = prepare_dataset(
            dataset_path=dataset_path,
            product_yaml=product_yaml,
            output_path=metadata_output_path,
        )

        # Write the dataset doc to file
        # to_path(metadata_output_path, dataset_doc)
        # log.info(f"Wrote dataset to {metadata_output_path}")

        # Convert dataset doc to stac item
        stac_item = to_stac_item(
            dataset=dataset_doc, stac_item_destination_url=str(stac_item_destination_url)
        )

        # Fix links in stac item
        stac_item = fix_proj_code_property(stac_item)

        # Write stac item
        if is_s3_path(stac_item_destination_url):
            s3_dump(
                data=json.dumps(stac_item, indent=2),
                url=stac_item_destination_url,
                ACL="bucket-owner-full-control",
                ContentType="application/json",
            )
        else:
            with open(stac_item_destination_url, "w") as file:
                json.dump(stac_item, file, indent=2)  # `indent=4` makes it human-readable

        log.info(f"STAC item written to {stac_item_destination_url}")
