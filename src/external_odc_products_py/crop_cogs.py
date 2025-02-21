"""
Crop Geotiffs to the extent of Africa
"""

import logging
import os
import sys
from pathlib import Path

import click
import geopandas as gpd
import numpy as np
import rioxarray
from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry
from odc.geo.xr import assign_crs, write_cog

from external_odc_products_py.io import (  # noqa F401
    check_directory_exists,
    check_file_exists,
    find_geotiff_files,
    get_filesystem,
)
from external_odc_products_py.logs import get_logger
from external_odc_products_py.wapor_v3 import get_mapset_rasters

log = get_logger(Path(__file__).stem, level=logging.INFO)

AFRICA_EXTENT_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent-bbox.json"


def reproject_geotiff(img_path: str, output_path: str):
    da = rioxarray.open_rasterio(img_path).squeeze(dim="band")
    crs = da.rio.crs
    nodata = da.rio.nodata

    da = assign_crs(da, crs)

    # Subset to Africa
    africa_extent = gpd.read_file(AFRICA_EXTENT_URL).to_crs(crs)
    africa_extent_geopolygon = Geometry(africa_extent.iloc[0].geometry, crs=africa_extent.crs)
    africa_extent_geobox = GeoBox.from_geopolygon(
        geopolygon=africa_extent_geopolygon,
        crs=da.odc.geobox.crs,
        resolution=da.odc.geobox.resolution,
    )

    # Reproject
    da = da.odc.reproject(africa_extent_geobox)

    # Create an in memory COG.
    cog_bytes = write_cog(geo_im=da, fname=":mem:", nodata=nodata, overview_resampling="nearest")

    # Write to file
    fs = get_filesystem(output_path, anon=False)
    with fs.open(output_path, "wb") as file:
        file.write(cog_bytes)
    log.info(f"Cropped geotiff written to {output_path}")


def crop_geotiff(img_path: str, output_path: str):
    da = rioxarray.open_rasterio(img_path).squeeze(dim="band")
    crs = da.rio.crs
    nodata = da.rio.nodata

    da = assign_crs(da, crs)

    # Subset to Africa
    africa_extent = gpd.read_file(AFRICA_EXTENT_URL).to_crs(crs)
    minx, miny, maxx, maxy = africa_extent.total_bounds
    # Note: lats are upside down!
    da = da.sel(y=slice(maxy, miny), x=slice(minx, maxx))

    # Create an in memory COG.
    cog_bytes = write_cog(geo_im=da, fname=":mem:", nodata=nodata, overview_resampling="nearest")

    # Write to file
    fs = get_filesystem(output_path, anon=False)
    with fs.open(output_path, "wb") as file:
        file.write(cog_bytes)
    log.info(f"Cropped geotiff written to {output_path}")


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
def crop_wapor_cogs(
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
