import json
import logging
import os
from pathlib import Path

import click
from eodatasets3.serialise import to_path  # noqa F401
from eodatasets3.stac import to_stac_item
from odc.aws import s3_dump

from external_odc_products_py import (
    iwmi_odr,
    wapor_v3,
)
from external_odc_products_py.io import (
    check_directory_exists,
    check_file_exists,
    find_geotiff_files,
    get_filesystem,
    is_gcsfs_path,
    is_s3_path,
    is_url,
)
from external_odc_products_py.logs import get_logger
from external_odc_products_py.utils import download_product_yaml

log = get_logger(Path(__file__).stem, level=logging.INFO)


@click.command()
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
    default=None,
    help="File path to the directory containing the COG files",
)
@click.option(
    "--stac-output-dir",
    type=str,
    help="Directory to write the stac files docs to",
)
@click.option("--overwrite/--no-overwrite", default=False)
def create_stac_files(
    product_name: str,
    product_yaml: str,
    geotiffs_dir: str,
    stac_output_dir: str,
    overwrite: bool,
):

    # Validate products
    valid_product_names = ["iwmi_blue_et_monthly", "iwmi_green_et_monthly", "wapor_soil_moisture"]
    if product_name not in valid_product_names:
        raise NotImplementedError(
            f"Stac file generation has not been implemented for {product_name}"
        )

    # Set to temporary dir as output metadata yaml files are not required.
    metadata_output_dir = "/tmp/metadata_docs"
    if product_name not in os.path.basename(metadata_output_dir.rstrip("/")):
        metadata_output_dir = os.path.join(metadata_output_dir, product_name)

    if is_s3_path(metadata_output_dir):
        raise RuntimeError("Metadata files require to be written to a local directory")
    else:
        metadata_output_dir = Path(metadata_output_dir).resolve()

    if not check_directory_exists(metadata_output_dir):
        fs = get_filesystem(metadata_output_dir, anon=False)
        fs.makedirs(metadata_output_dir, exist_ok=True)
        log.info(f"Created the directory {metadata_output_dir}")

    # Path to product yaml
    if not is_s3_path(product_yaml):
        if is_url(product_yaml):
            product_yaml = download_product_yaml(product_yaml)
        else:
            product_yaml = Path(product_yaml).resolve()
    else:
        NotImplemented("Product yaml is expected to be a local file or url not s3 path")

    # Directory to write the stac files to
    if product_name not in os.path.basename(stac_output_dir.rstrip("/")):
        stac_output_dir = os.path.join(stac_output_dir, product_name)

    if not is_s3_path(stac_output_dir):
        stac_output_dir = Path(stac_output_dir).resolve()

    if not check_directory_exists(stac_output_dir):
        fs = get_filesystem(stac_output_dir, anon=False)
        fs.makedirs(stac_output_dir, exist_ok=True)
        log.info(f"Created the directory {stac_output_dir}")

    # Geotiffs directory
    if geotiffs_dir:
        # Find all the geotiffs files in the directory
        geotiffs = find_geotiff_files(geotiffs_dir)
    else:
        if product_name.startswith("wapor"):
            if product_name == "wapor_soil_moisture":
                mapset_code = "L2-RSM-D"

            geotiffs = wapor_v3.get_mapset_rasters(mapset_code)
            # Use a gsutil URI instead of the the public URL
            geotiffs = [i.replace("https://storage.googleapis.com/", "gs://") for i in geotiffs]
        else:
            raise ValueError("No file path to the directory containing the COG files provided")

    log.info(f"Found {len(geotiffs)} geotiffs")

    log.info(f"Generating stac files for the product {product_name}")

    for idx, geotiff in enumerate(geotiffs):
        log.info(f"Generating stac file for {geotiff} {idx+1}/{len(geotiffs)}")

        # File system Path() to the dataset
        # or gsutil URI prefix  (gs://bucket/key) to the dataset.
        if not is_s3_path(geotiff) and not is_gcsfs_path(geotiff):
            dataset_path = Path(geotiff)
        else:
            dataset_path = geotiff

        tile_id = os.path.basename(dataset_path).removesuffix(".tif")

        if product_name.startswith("wapor"):
            year, month, _ = tile_id.split(".")[-1].split("-")
            metadata_output_path = Path(
                os.path.join(metadata_output_dir, year, month, f"{tile_id}.odc-metadata.yaml")
            )
            stac_item_destination_url = os.path.join(
                stac_output_dir, year, month, f"{tile_id}.stac-item.json"
            )
        else:
            metadata_output_path = Path(
                os.path.join(metadata_output_dir, f"{tile_id}.odc-metadata.yaml")
            )
            stac_item_destination_url = os.path.join(stac_output_dir, f"{tile_id}.stac-item.json")

        # Check if the stac item exist:
        if not overwrite:
            if check_file_exists(stac_item_destination_url):
                log.info(
                    f"{stac_item_destination_url} exists! Skipping stac file generation for {dataset_path}"
                )
                continue

        # Prepare the dataset's metadata doc
        if product_name.startswith("iwmi"):
            dataset_doc = iwmi_odr.prepare_dataset(
                dataset_path=dataset_path,
                product_yaml=product_yaml,
                output_path=metadata_output_path,
            )
        elif product_name.startswith("wapor"):
            dataset_doc = wapor_v3.prepare_dataset(
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
        if product_name.startswith("wapor"):
            assets = stac_item["assets"]
            for band in assets.keys():
                band_url = assets[band]["href"]
                if band_url.startswith("gs://"):
                    new_band_url = band_url.replace("gs://", "https://storage.googleapis.com/")
                    stac_item["assets"][band]["href"] = new_band_url

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

        print("manual break")
        break


if __name__ == "__main__":
    create_stac_files()
