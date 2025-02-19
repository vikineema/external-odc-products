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

from eodatasets3.images import ValidDataMethod
from eodatasets3.model import DatasetDoc

from external_odc_products_py.easi_assemble import EasiPrepare
from external_odc_products_py.logs import get_logger
from external_odc_products_py.utils import get_last_modified, odc_uuid

logger = get_logger(Path(__file__).stem, level=logging.INFO)


# Static namespace (seed) to generate uuids for datacube indexing
# Get a new seed value for a new driver from uuid4():
# Python terminal
# >>> import uuid
# >>> uuid.uuid4()
# UUID_NAMESPACE = uuid.UUID("413ed7e6-75c7-4da9-92dc-1a0205c7be53")


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
    extension = "tif"

    tile_id = os.path.basename(dataset_path).removesuffix(f".{extension}")

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
    p.dataset_version = "v1.0.0"
    # Unique dataset UUID built from the unique Product ID
    p.dataset_id = odc_uuid(p.product_name, p.dataset_version, [unique_name])

    ## Satellite, Instrument and Processing level
    # High-level name for the source data (satellite platform or project name).
    # Comma-separated for multiple platforms.
    p.platform = "DIWASA"
    # p.instrument = 'SAMPLETYPE'  #  Instrument name, optional
    # Organisation that produces the data.
    # URI domain format containing a '.'
    p.producer = "www.iwmi.cgiar.org"
    # ODC/EASI identifier for this "family" of products, optional
    # p.product_family = 'FAMILY_STUFF'
    p.properties["odc:file_format"] = file_format  # Helpful but not critical
    p.properties["odc:product"] = p.product_name

    ## Scene capture and Processing

    # Datetime derived from file name
    # The input string
    date_string = tile_id.split("_")[-1]
    # Convert to a datetime object
    date_object = datetime.strptime(date_string, "%Y.%m.%d")
    # Searchable datetime of the dataset, datetime object
    p.datetime = date_object
    # Searchable start and end datetimes of the dataset, datetime objects
    # p.datetime_range = ('OPTIONAL', 'OPTIONAL')
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
    p.note_measurement("data", dataset_path, relative_to_metadata=False)

    return p.to_dataset_doc(validate_correctness=True, sort_measurements=True)
