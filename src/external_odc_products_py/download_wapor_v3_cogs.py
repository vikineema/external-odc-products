"""
Download the WaPOR version 3 mapset rasters and ,
convert to Cloud Optimized Geotiff, and push to an S3 bucket.

"""

import logging
import os
import sys
from pathlib import Path
from subprocess import STDOUT, check_output
from urllib.parse import urlparse

import click
import numpy as np
from odc.aws import s3_dump

from external_odc_products_py.io import (
    check_directory_exists,
    check_file_exists,
    get_filesystem,
    is_s3_path,
)
from external_odc_products_py.logs import get_logger
from external_odc_products_py.wapor_v3_metadata import get_mapset_rasters

LOCAL_DOWNLOAD_DIR = "tmp/wapor_v3"

log = get_logger(Path(__file__).stem, level=logging.INFO)


def get_path_with_handler(url):
    """
    Get the gdal file system handler for a path
    """
    o = urlparse(url)

    bucket = o.netloc
    key = o.path

    if o.scheme in ["http", "https"]:
        # /vsicurl/http[s]://path/to/remote/resource
        path_with_handler = os.path.join("/vsicurl/", url)
    elif o.scheme in ["s3"]:
        # /vsis3/bucket/key
        path_with_handler = os.path.join("/vsis3/", bucket, key.lstrip("/"))
    elif o.scheme in ["gcs", "gs"]:
        # /vsigs/bucket/key
        path_with_handler = os.path.join("/vsigs/", bucket, key.lstrip("/"))
    if o.scheme == "" or o.scheme == "file":
        path_with_handler = os.path.abspath(url)

    return path_with_handler


def crop_and_upload_cog(img_path: str, output_path: str):
    """
    Crop GeoTIFF to Africa extent then create a COG and upload to S#

    This manages memory better than using the crop_geotiff from utils.py
    Able to use this on 16GB RAM machine.
    """
    # Temporary directory to store the clipped geotiffs
    local_cropped_tiffs_dir = os.path.join(LOCAL_DOWNLOAD_DIR, "cropped_geotiffs")
    if not check_directory_exists(local_cropped_tiffs_dir):
        fs = get_filesystem(local_cropped_tiffs_dir, anon=False)
        fs.makedirs(local_cropped_tiffs_dir, exist_ok=True)

    # Crop GeoTIFF and save to disk
    temp_geotiff = os.path.join(local_cropped_tiffs_dir, os.path.basename(output_path))
    cmd = f"gdal_translate -projwin -26.36 38.35 64.50 -47.97 -projwin_srs EPSG:4326 \
    {get_path_with_handler(img_path)} {temp_geotiff}"
    check_output(cmd, stderr=STDOUT, shell=True)
    log.info(f"File {temp_geotiff} croppped successfully")

    # Create a COG file from cropped GeoTIFF and save to disk
    if is_s3_path(output_path):
        # Temporary directory to store the cogs before uploading to s3
        local_cog_dir = os.path.join(LOCAL_DOWNLOAD_DIR, "cogs")
        if not check_directory_exists(local_cog_dir):
            fs = get_filesystem(local_cog_dir, anon=False)
            fs.makedirs(local_cog_dir, exist_ok=True)

        # Create a COG and save to local disk
        cloud_optimised_file = os.path.join(local_cog_dir, os.path.basename(output_path))
        cmd = (
            f"rio cogeo create --overview-resampling nearest {temp_geotiff} {cloud_optimised_file}"
        )
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"File {cloud_optimised_file} cloud optimised successfully")

        # Uppload COG to s3
        log.info(f"Upload {cloud_optimised_file} to S3 {output_path}")
        s3_dump(
            data=open(str(cloud_optimised_file), "rb").read(),
            url=output_path,
            ACL="bucket-owner-full-control",
            ContentType="image/tiff",
        )
        log.info(f"File written to {output_path}")
    else:
        cmd = f"rio cogeo create --overview-resampling nearest {temp_geotiff} {output_path}"
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"File {output_path} cloud optimised successfully")


@click.command()
@click.option(
    "--mapset-code",
    type=str,
    default=None,
    help="WaPOR version 3 mapset to crop COG files for.",
)
@click.option(
    "--output-dir",
    type=str,
    help="Directory to write the cropped COG files to",
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
def download_wapor_v3_cogs(
    mapset_code: str, output_dir: str, overwrite: bool, max_parallel_steps: int, worker_idx: int
):

    all_geotiff_files = get_mapset_rasters(mapset_code)
    # Use a gsutil URI instead of the the public URL
    all_geotiff_files = [
        i.replace("https://storage.googleapis.com/", "gs://") for i in all_geotiff_files
    ]

    # Split files equally among the workers
    task_chunks = np.array_split(np.array(all_geotiff_files), max_parallel_steps)
    task_chunks = [chunk.tolist() for chunk in task_chunks]
    task_chunks = list(filter(None, task_chunks))

    # In case of the index being bigger than the number of positions in the array, the extra POD isn' necessary
    if len(task_chunks) <= worker_idx:
        log.warning(f"Worker {worker_idx} Skipped!")
        sys.exit(0)

    log.info(f"Executing worker {worker_idx}")

    geotiffs = task_chunks[worker_idx]

    log.info(f"Creating COGs for {len(geotiffs)} geotiffs")

    for idx, geotiff in enumerate(geotiffs):
        log.info(f"Proceesing {geotiff} {idx+1}/{len(geotiffs)}")

        tile_id = os.path.basename(geotiff).removesuffix(".tif")

        try:
            year, month, _ = tile_id.split(".")[-1].split("-")
        except ValueError:
            year, month = tile_id.split(".")[-1].split("-")

        output_cog_path = os.path.join(output_dir, year, month, f"{tile_id}.tif")
        if not overwrite:
            if check_file_exists(output_cog_path):
                log.info(
                    f"{output_cog_path} exists! Skipping stac file generation for {output_cog_path}"
                )
                continue

        # Create the required parent directories
        output_cog_parent_dir = os.path.dirname(output_cog_path)
        if not check_directory_exists(output_cog_parent_dir):
            fs = get_filesystem(output_cog_parent_dir, anon=False)
            fs.makedirs(output_cog_parent_dir, exist_ok=True)

        crop_and_upload_cog(geotiff, output_cog_path)
