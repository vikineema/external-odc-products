"""
Crop Geotiffs to the extent of Africa
"""

import logging
from pathlib import Path

import geopandas as gpd
import rioxarray
from odc.geo.xr import assign_crs, write_cog

from external_odc_products_py.io import get_filesystem
from external_odc_products_py.logs import get_logger

log = get_logger(Path(__file__).stem, level=logging.INFO)

AFRICA_EXTENT_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent-bbox.json"


def crop_image(img_path: str, output_path: str):
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
