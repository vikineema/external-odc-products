import json
import logging
import sys
from pathlib import Path

import click
import datacube
import yaml
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools.utils import (
    allow_unsafe,
    archive_less_mature,
    index_update_dataset,
    publish_action,
    statsd_gauge_reporting,
    statsd_setting,
    transform_stac,
    update_if_exists_flag,
)

from external_odc_products_py.stac_to_eo3 import stac_transform

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)


@click.command("fs-to-dc")
@click.argument("input_directory", type=str, nargs=1)
@update_if_exists_flag
@allow_unsafe
@archive_less_mature
@transform_stac
@statsd_setting
@publish_action
@click.option(
    "--glob",
    default=None,
    help="File system glob to use, defaults to **/*.yaml or **/*.json for STAC.",
)
@click.argument("product", type=str, nargs=1, required=False)
def cli(
    input_directory,
    update_if_exists,
    allow_unsafe,
    stac,
    statsd_setting,
    glob,
    archive_less_mature,
    publish_action,
    product,
):
    """
    Iterate through files in a local folder and add them to datacube.

    Product is optional; if one is provided, it must match all datasets.
    Can provide a single product name or a space separated list of multiple products
    (formatted as a single string).
    """
    dc = datacube.Datacube()

    # Check datacube connection and products
    candidate_products = product.split()
    odc_products = dc.list_products().name.values

    odc_products = set(odc_products)
    if not set(candidate_products).issubset(odc_products):
        missing_products = list(set(candidate_products) - odc_products)
        print(
            f"Error: Requested Product/s {', '.join(missing_products)} "
            f"{'is' if len(missing_products) == 1 else 'are'} "
            "not present in the ODC Database",
            file=sys.stderr,
        )
        sys.exit(1)

    doc2ds = Doc2Dataset(dc.index, products=candidate_products)

    if glob is None:
        glob = "**/*.json" if stac else "**/*.yaml"

    files_to_process = Path(input_directory).glob(glob)

    added, failed = 0, 0

    for in_file in files_to_process:
        with in_file.open() as f:
            try:
                if in_file.suffix in (".yml", ".yaml"):
                    metadata = yaml.safe_load(f)
                else:
                    metadata = json.load(f)
                # Do the STAC Transform if it's flagged
                stac_doc = None
                if stac:
                    stac_doc = metadata
                    metadata = stac_transform(metadata)
                index_update_dataset(
                    metadata,
                    in_file.absolute().as_uri(),
                    dc=dc,
                    doc2ds=doc2ds,
                    update_if_exists=update_if_exists,
                    allow_unsafe=allow_unsafe,
                    archive_less_mature=archive_less_mature,
                    publish_action=publish_action,
                    stac_doc=stac_doc,
                )
                added += 1
            except Exception:  # pylint: disable=broad-except
                logging.exception("Failed to add dataset %s", in_file)
                failed += 1

    logging.info("Added %s and failed %s datasets.", added, failed)
    if statsd_setting:
        statsd_gauge_reporting(added, ["app:fs_to_dc", "action:added"], statsd_setting)
        statsd_gauge_reporting(failed, ["app:fs_to_dc", "action:failed"], statsd_setting)


if __name__ == "__main__":
    cli()
