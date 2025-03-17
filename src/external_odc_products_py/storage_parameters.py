import json
import logging
import os
from pathlib import Path

import click
import rioxarray
from tqdm import tqdm

from external_odc_products_py.io import (
    check_directory_exists,
    find_geotiff_files,
    get_filesystem,
)
from external_odc_products_py.logs import get_logger
from external_odc_products_py.wapor_v3.prepare_metadata import get_mapset_rasters

logger = get_logger(Path(__file__).stem, level=logging.INFO)


@click.command()
@click.option(
    "--product-name",
    type=str,
    help="Name of the product to generate the stac item files for",
)
@click.option(
    "--geotiffs-dir",
    type=str,
    default=None,
    help="File path to the directory containing the COG files",
)
@click.option(
    "--output-dir",
    type=str,
    help="Directory to write the unique storage parameters text file to",
)
def get_storage_parameters(
    product_name: str,
    geotiffs_dir: str,
    output_dir: str,
):

    # Geotiffs directory
    if geotiffs_dir:
        # Find all the geotiffs files in the directory
        geotiffs_file_paths = find_geotiff_files(geotiffs_dir)
    else:
        if product_name.startswith("wapor"):
            if product_name == "wapor_soil_moisture":
                mapset_code = "L2-RSM-D"
            elif product_name == "wapor_monthly_npp":
                mapset_code = "L2-NPP-M"
            geotiffs_file_paths = get_mapset_rasters(mapset_code)
            # Use a gsutil URI instead of the the public URL
            geotiffs_file_paths = [
                i.replace("https://storage.googleapis.com/", "gs://") for i in geotiffs_file_paths
            ]
        else:
            raise ValueError("No file path to the directory containing the COG files provided")

    logger.info(f"Found {len(geotiffs_file_paths)} geotiff files")
    storage_parameters_list = []

    for file_path in tqdm(iterable=geotiffs_file_paths, total=len(geotiffs_file_paths)):
        da = rioxarray.open_rasterio(file_path)
        crs = da.rio.crs.to_epsg()  # Coordinate Reference System
        res_x, res_y = da.rio.resolution()  # Pixel resolution (x, y)
        dtype = str(da.dtype)  # Data type of the first band
        nodata = da.rio.nodata
        attrs = da.attrs
        add_offset = attrs.get("add_offset", None)
        scale_factor = attrs.get("scale_factor", None)

        item = {
            "crs": f"EPSG:{crs}",
            "res_x": str(res_x),
            "res_y": str(res_y),
            "add_offset": str(add_offset),
            "scale_factor": str(scale_factor),
            "dtype": dtype,
            "nodata": str(nodata),
        }
        storage_parameters_list.append(item)

    # Convert dicts to JSON strings to create a unique set
    unique_storage_parameters = [
        json.loads(s) for s in {json.dumps(d, sort_keys=True) for d in storage_parameters_list}
    ]
    storage_parameters_json_array = json.dumps(unique_storage_parameters)

    output_file = os.path.join(output_dir, f"{product_name}_storage_parameters")

    fs = get_filesystem(path=output_dir, anon=False)
    if not check_directory_exists(path=output_dir):
        fs.mkdirs(path=output_dir, exist_ok=True)
        logger.info(f"Created directory {output_dir}")

    with fs.open(output_file, "w") as file:
        file.write(storage_parameters_json_array)
    logger.info(f"Tasks chunks written to {output_file}")
