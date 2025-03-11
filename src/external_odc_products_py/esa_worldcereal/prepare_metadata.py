#!python3
# Prepare eo3 metadata for one SAMPLE DATASET.
#
## Main steps
# 1. Populate EasiPrepare class from source metadata
# 2. Call p.write_eo3() to validate and write the dataset YAML document

import logging
import os
import re
from datetime import datetime
from pathlib import Path

import rioxarray
from eodatasets3.images import ValidDataMethod
from eodatasets3.model import DatasetDoc

from external_odc_products_py.easi_assemble import EasiPrepare
from external_odc_products_py.io import find_geotiff_files
from external_odc_products_py.logs import get_logger
from external_odc_products_py.utils import odc_uuid

log = get_logger(Path(__file__).stem, level=logging.INFO)


# Static namespace (seed) to generate uuids for datacube indexing
# Get a new seed value for a new driver from uuid4():
# Python terminal
# >>> import uuid
# >>> uuid.uuid4()
# UUID_NAMESPACE = uuid.UUID("2f21a418-06e3-49b0-91d0-5e218f0c0b58")


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
    ## Initialise and validate inputs
    # Creates variables (see EasiPrepare for others):
    # - p.dataset_path
    # - p.product_name
    p = EasiPrepare(dataset_path, product_yaml, output_path)

    ## File format of preprocessed data
    # e.g. cloud-optimised GeoTiff (= GeoTiff)
    file_format = "GeoTIFF"
    extension = "tif"

    ## Check the p.dataset_path
    # Use a glob or a file PATTERN.
    # Customise depending on the expected dir/file names and p.dataset_path
    files = find_geotiff_files(str(p.dataset_path))
    if not files:
        return False, f"Product ID does not match expected form: {p.dataset_path}"

    ## IDs and Labels

    # AEZ-based GeoTIFF files inside are named according to following convention
    # {AEZ_id}_{season}_{product}_{startdate}_{enddate}_{classification|confidence}.tif
    AEZ_id, season, product, startdate, enddate, _ = (
        os.path.basename(files[0]).removesuffix(f".{extension}").split("_")
    )

    # Unique dataset name, probably parsed from p.dataset_path or a filename
    unique_name = f"{AEZ_id}_{season}_{product}_{startdate}_{enddate}"

    # Can not have '.' in label
    unique_name_replace = re.sub("\.", "_", unique_name)
    label = f"{unique_name_replace}-{p.product_name}"  # noqa F841
    # p.label = label

    # product_name is added by EasiPrepare().init()
    p.product_uri = f"https://explorer.digitalearth.africa/product/{p.product_name}"

    # The version of the source dataset
    p.dataset_version = "v1.0.0"

    # Unique dataset UUID built from the unique Product ID
    p.dataset_id = odc_uuid(p.product_name, p.dataset_version, [unique_name])

    ## Satellite, Instrument and Processing level

    # High-level name for the source data (satellite platform or project name).
    # Comma-separated for multiple platforms.
    p.platform = "ESA WorldCereal project"
    #  Instrument name, optional
    # p.instrument = 'OPTIONAL'
    # Organisation that produces the data.
    # URI domain format containing a '.'
    p.producer = "https://vito.be/"
    # ODC/EASI identifier for this "family" of products, optional
    # p.product_family = 'OPTIONAL'
    # Helpful but not critical
    p.properties["odc:file_format"] = file_format
    p.properties["odc:product"] = p.product_name

    ## Scene capture and Processing

    # Use attributes from the classification measurement geotiff instead
    # of the confidence measurement
    band_regex = rf"([^_]+)\.{extension}$"
    measurement_map = p.map_measurements_to_paths(band_regex)
    for measurement_name, file_location in measurement_map.items():
        if measurement_name == "classification":
            attrs = rioxarray.open_rasterio(str(file_location)).attrs

    # Searchable datetime of the dataset, datetime object
    p.datetime = datetime.strptime(attrs["start_date"], "%Y-%m-%d")
    # Searchable start and end datetimes of the dataset, datetime objects
    p.datetime_range = (
        datetime.strptime(attrs["start_date"], "%Y-%m-%d"),
        datetime.strptime(attrs["end_date"], "%Y-%m-%d").replace(hour=23, minute=59, second=59),
    )
    # When the source dataset was created by the producer, datetime object
    p.processed = datetime.strptime(attrs["creation_time"], "%Y-%m-%d %H:%M:%S")

    ## Geometry
    # Geometry adds a "valid data" polygon for the scene, which helps bounding box searching in ODC
    # Either provide a "valid data" polygon or calculate it from all bands in the dataset
    # ValidDataMethod.thorough = Vectorize the full valid pixel mask as-is
    # ValidDataMethod.filled = Fill holes in the valid pixel mask before vectorizing
    # ValidDataMethod.convex_hull = Take convex-hull of valid pixel mask before vectorizing
    # ValidDataMethod.bounds = Use the image file bounds, ignoring actual pixel values
    # p.geometry = Provide a "valid data" polygon rather than read from the file, shapely.geometry.base.BaseGeometry()
    # p.crs = Provide a CRS string if measurements GridSpec.crs is None, "epsg:*" or WKT
    p.valid_data_method = ValidDataMethod.bounds

    ## Scene metrics, as available

    # The "region" of acquisition, if applicable
    p.region_code = str(attrs["AEZ_ID"])
    # p.properties["eo:gsd"] = 'FILL'  # Nominal ground sample distance or spatial resolution
    # p.properties["eo:cloud_cover"] = 'OPTIONAL'
    # p.properties["eo:sun_azimuth"] = 'OPTIONAL'
    # p.properties["eo:sun_zenith"] = 'OPTIONAL'

    ## Product-specific properties, OPTIONAL
    # For examples see eodatasets3.properties.Eo3Dict().KNOWN_PROPERTIES
    # p.properties[f'{custom_prefix}:algorithm_version'] = ''
    # p.properties[f'{custom_prefix}:doi'] = ''
    # p.properties[f'{custom_prefix}:short_name'] = ''
    # p.properties[f'{custom_prefix}:processing_system'] = ''

    ## Add measurement paths
    for measurement_name, file_location in measurement_map.items():
        log.debug(f"Measurement map: {measurement_name} > {file_location}")
        p.note_measurement(measurement_name, file_location, relative_to_metadata=False)
    return p.to_dataset_doc(validate_correctness=True, sort_measurements=True)
