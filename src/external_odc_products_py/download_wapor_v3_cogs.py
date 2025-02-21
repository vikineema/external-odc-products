"""
Download the WaPOR version 3 mapset rasters and ,
convert to Cloud Optimized Geotiff, and push to an S3 bucket.

"""

import logging
import os
import sys
from pathlib import Path

import click
import numpy as np

from external_odc_products_py.io import (
    check_directory_exists,
    check_file_exists,
    get_filesystem,
)
from external_odc_products_py.logs import get_logger
from external_odc_products_py.utils import crop_geotiff
from external_odc_products_py.wapor_v3_metadata import get_mapset_rasters

log = get_logger(Path(__file__).stem, level=logging.INFO)


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
            log.info(f"Created the directory {output_cog_parent_dir}")

        crop_geotiff(geotiff, output_cog_path)
