#!python3
# Prepare eo3 metadata for one SAMPLE DATASET.
#
## Main steps
# 1. Populate EasiPrepare class from source metadata
# 2. Call p.write_eo3() to validate and write the dataset YAML document

import calendar
import collections
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from eodatasets3.images import ValidDataMethod
from eodatasets3.model import DatasetDoc

from external_odc_products_py.easi_assemble import EasiPrepare
from external_odc_products_py.io import find_geotiff_files
from external_odc_products_py.logs import get_logger
from external_odc_products_py.utils import get_last_modified, odc_uuid

log = get_logger(Path(__file__).stem, level=logging.INFO)


# Static namespace (seed) to generate uuids for datacube indexing
# Get a new seed value for a new driver from uuid4():
# Python terminal
# >>> import uuid
# >>> uuid.uuid4()
# UUID_NAMESPACE = uuid.UUID("2f21a418-06e3-49b0-91d0-5e218f0c0b58")


def get_WaPORv3_info(url: str) -> pd.DataFrame:
    """
    Get information on WaPOR v3 data from the api url.
    WaPOR v3 variables are stored in `mapsets`, which in turn contain
    `rasters` that contain the data for a particular date or period.

    Parameters
    ----------
    url : str
        URL to get information from
    Returns
    -------
    pd.DataFrame
        A table of the mapset attributes found.
    """
    data = {"links": [{"rel": "next", "href": url}]}

    output_dict = collections.defaultdict(list)
    while "next" in [x["rel"] for x in data["links"]]:
        url_ = [x["href"] for x in data["links"] if x["rel"] == "next"][0]
        response = requests.get(url_)
        response.raise_for_status()
        data = response.json()["response"]
        for item in data["items"]:
            for key in list(item.keys()):
                if key == "links":
                    output_dict[key].append(item[key][0]["href"])
                else:
                    output_dict[key].append(item[key])

    output_df = pd.DataFrame(output_dict)

    if "code" in output_df.columns:
        output_df.sort_values("code", inplace=True)
        output_df.reset_index(drop=True, inplace=True)
    return output_df


def get_mapset_rasters_from_api(wapor_v3_mapset_code: str) -> list[str]:
    base_url = "https://data.apps.fao.org/gismgr/api/v2/catalog/workspaces/WAPOR-3/mapsets"
    wapor_v3_mapset_url = os.path.join(base_url, wapor_v3_mapset_code, "rasters")
    wapor_v3_mapset_rasters = get_WaPORv3_info(wapor_v3_mapset_url)["downloadUrl"].to_list()
    return wapor_v3_mapset_rasters


def get_mapset_rasters_from_gsutil_uri(wapor_v3_mapset_code: str) -> list[str]:
    base_url = "gs://fao-gismgr-wapor-3-data/DATA/WAPOR-3/MAPSET/"
    wapor_v3_mapset_url = os.path.join(base_url, wapor_v3_mapset_code)
    wapor_v3_mapset_rasters = find_geotiff_files(directory_path=wapor_v3_mapset_url)
    return wapor_v3_mapset_rasters


def get_mapset_rasters(wapor_v3_mapset_code: str) -> list[str]:
    try:
        wapor_v3_mapset_rasters = get_mapset_rasters_from_api(wapor_v3_mapset_code)
    except Exception:
        wapor_v3_mapset_rasters = get_mapset_rasters_from_gsutil_uri(wapor_v3_mapset_code)
    log.info(f"Found {len(wapor_v3_mapset_rasters)} rasters for the mapset {wapor_v3_mapset_code}")
    return wapor_v3_mapset_rasters


def get_dekad(year: str | int, month: str | int, dekad_label: str) -> tuple:
    """
    Get the end date of the dekad that a date belongs to and the time range
    for the dekad.
    Every month has three dekads, such that the first two dekads
    have 10 days (i.e., 1-10, 11-20), and the third is comprised of the
    remaining days of the month.

    Parameters
    ----------
    year: int | str
        Year of the dekad
    month: int | str
        Month of the dekad
    dekad_label: str
        Label indicating whether the date falls in the 1st, 2nd or 3rd dekad
        in a month

    Returns
    -------
    tuple
        The end date of the dekad and the time range for the dekad.
    """
    if isinstance(year, str):
        year = int(year)

    if isinstance(month, str):
        month = int(month)

    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])

    d1_start_date, d2_start_date, d3_start_date = pd.date_range(
        start=first_day, end=last_day, freq="10D", inclusive="left"
    )
    if dekad_label == "D1":
        input_datetime = (d2_start_date - relativedelta(days=1)).to_pydatetime()
        start_datetime = d1_start_date.to_pydatetime()
        end_datetime = input_datetime.replace(hour=23, minute=59, second=59)
    elif dekad_label == "D2":
        input_datetime = (d3_start_date - relativedelta(days=1)).to_pydatetime()
        start_datetime = d2_start_date.to_pydatetime()
        end_datetime = input_datetime.replace(hour=23, minute=59, second=59)
    elif dekad_label == "D3":
        input_datetime = last_day
        start_datetime = d3_start_date.to_pydatetime()
        end_datetime = input_datetime.replace(hour=23, minute=59, second=59)

    return input_datetime, (start_datetime, end_datetime)


def prepare_dataset(
    dataset_path: str | Path,
    product_yaml: str | Path,
    output_path: str = None,
) -> DatasetDoc:
    """
    Prepare an eo3 metadata file for SAMPLE data product.
    @param dataset_path: Path to the geotiff to create dataset metadata for.
    @param product_yaml: Path to the product definition yaml file.
    @param output_path: Path to write the output metadata file.

    :return: DatasetDoc
    """
    ## File format of data
    # e.g. cloud-optimised GeoTiff (= GeoTiff)
    file_format = "GeoTIFF"
    file_extension = ".tif"

    tile_id = os.path.basename(dataset_path).removesuffix(file_extension)

    ## Initialise and validate inputs
    # Creates variables (see EasiPrepare for others):
    # - p.dataset_path
    # - p.product_name
    # The output_path and tile_id are use to create a dataset unique filename
    # for the output metadata file.
    # Variable p is a dictionary of metadata and measurements to be written
    # to the output metadata file.
    # The code will populate p with the metadata and measurements and then call
    # p.write_eo3() to write the output metadata file.
    p = EasiPrepare(dataset_path, product_yaml, output_path)

    ## IDs and Labels should be dataset and Product unique
    # Unique dataset name, probably parsed from p.dataset_path or a filename
    unique_name = f"{tile_id}"
    # Can not have '.' in label
    unique_name_replace = re.sub("\.", "_", unique_name)
    label = f"{unique_name_replace}-{p.product_name}"  # noqa F841
    # p.label = label # Optional
    # product_name is added by EasiPrepare().init()
    p.product_uri = f"https://explorer.digitalearth.africa/product/{p.product_name}"
    # The version of the source dataset
    p.dataset_version = "v3.0"
    # Unique dataset UUID built from the unique Product ID
    p.dataset_id = odc_uuid(p.product_name, p.dataset_version, [unique_name])

    ## Satellite, Instrument and Processing level
    # High-level name for the source data (satellite platform or project name).
    # Comma-separated for multiple platforms.
    p.platform = "WaPORv3"
    # p.instrument = 'SAMPLETYPE'  #  Instrument name, optional
    # Organisation that produces the data.
    # URI domain format containing a '.'
    p.producer = "www.fao.org"
    # ODC/EASI identifier for this "family" of products, optional
    # p.product_family = 'FAMILY_STUFF'
    p.properties["odc:file_format"] = file_format  # Helpful but not critical
    p.properties["odc:product"] = p.product_name

    ## Scene capture and Processing

    # Datetime derived from file name
    year, month, dekad_label = tile_id.split(".")[-1].split("-")
    input_datetime, time_range = get_dekad(year, month, dekad_label)
    # Searchable datetime of the dataset, datetime object
    p.datetime = input_datetime
    # Searchable start and end datetimes of the dataset, datetime objects
    p.datetime_range = time_range
    # When the source dataset was created by the producer, datetime object
    processed_dt = get_last_modified(dataset_path)
    if processed_dt:
        p.processed = processed_dt

    ## Geometry
    # Geometry adds a "valid data" polygon for the scene, which helps bounding box searching in ODC
    # Either provide a "valid data" polygon or calculate it from all bands in the dataset
    # Some techniques are more accurate than others, but all are valid. You may need to use coarser methods if the data
    # is particularly noisy or sparse.
    # ValidDataMethod.thorough = Vectorize the full valid pixel mask as-is
    # ValidDataMethod.filled = Fill holes in the valid pixel mask before vectorizing
    # ValidDataMethod.convex_hull = Take convex-hull of valid pixel mask before vectorizing
    # ValidDataMethod.bounds = Use the image file bounds, ignoring actual pixel values
    # p.geometry = Provide a "valid data" polygon rather than read from the file, shapely.geometry.base.BaseGeometry()
    # p.crs = Provide a CRS string if measurements GridSpec.crs is None, "epsg:*" or WKT
    p.valid_data_method = ValidDataMethod.bounds

    ## Product-specific properties, OPTIONAL
    # For examples see eodatasets3.properties.Eo3Dict().KNOWN_PROPERTIES
    # p.properties[f'{custom_prefix}:algorithm_version'] = ''
    # p.properties[f'{custom_prefix}:doi'] = ''
    # p.properties[f'{custom_prefix}:short_name'] = ''
    # p.properties[f'{custom_prefix}:processing_system'] = 'SomeAwesomeProcessor' # as an example

    ## Add measurement paths
    # This simple loop will go through all the measurements and determine their grids, the valid data polygon, etc
    # and add them to the dataset.
    # For LULC there is only one measurement, land_cover_class
    if p.product_name == "wapor_soil_moisture":
        p.note_measurement("relative_soil_moisture", dataset_path, relative_to_metadata=False)

    return p.to_dataset_doc(validate_correctness=True, sort_measurements=True)
