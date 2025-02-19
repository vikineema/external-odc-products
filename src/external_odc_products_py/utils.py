# Copied from https://github.com/vikineema/deafrica-scripts/blob/main/deafrica/utils.py
import logging
import os
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Sequence
from uuid import UUID, uuid5

import requests
import yaml
from odc.aws import s3_url_parse

from external_odc_products_py.io import (
    check_directory_exists,
    get_filesystem,
    is_gcsfs_path,
    is_s3_path,
    is_url,
)
from external_odc_products_py.logs import get_logger

log = get_logger(Path(__file__).stem, level=logging.INFO)


def odc_uuid(
    algorithm: str,
    algorithm_version: str,
    sources: Sequence[UUID],
    deployment_id: str = "",
    **other_tags,
) -> UUID:
    """
    Generate deterministic UUID for a derived Dataset.

    :param algorithm: Name of the algorithm
    :param algorithm_version: Version string of the algorithm
    :param sources: Sequence of input Dataset UUIDs
    :param deployment_id: Some sort of identifier for installation that performs
                          the run, for example Docker image hash, or dea module version on NCI.
    :param **other_tags: Any other identifiers necessary to uniquely identify dataset
    """
    tags = [f"{k}={str(v)}" for k, v in other_tags.items()]

    stringified_sources = (
        [str(algorithm), str(algorithm_version), str(deployment_id)]
        + sorted(tags)
        + [str(u) for u in sorted(sources)]
    )

    srcs_hashes = "\n".join(s.lower() for s in stringified_sources)
    return uuid5(UUID("6f34c6f4-13d6-43c0-8e4e-42b6c13203af"), srcs_hashes)


def download_product_yaml(url: str) -> str:
    try:
        # Create output directory
        tmp_products_dir = "/tmp/products"
        if not check_directory_exists(tmp_products_dir):
            fs = get_filesystem(tmp_products_dir, anon=False)
            fs.makedirs(tmp_products_dir, exist_ok=True)
            log.info(f"Created the directory {tmp_products_dir}")

        output_path = os.path.join(tmp_products_dir, os.path.basename(url))

        # Load product definition from url
        response = requests.get(url)
        response.raise_for_status()
        content = yaml.safe_load(response.content.decode(response.encoding))

        # Write to file.
        yaml_string = yaml.dump(
            content,
            default_flow_style=False,  # Ensures block format
            sort_keys=False,  # Keeps the original order
            allow_unicode=True,  # Ensures special characters are correctly represented
        )
        # Ensure it starts with "---"
        yaml_string = f"---\n{yaml_string}"

        with open(output_path, "w") as file:
            file.write(yaml_string)
        log.info(f"Product definition file written to {output_path}")
        return Path(output_path).resolve()
    except Exception as e:
        log.error(e)
        raise e


def s3_uri_to_public_url(s3_uri, region="af-south-1"):
    """Convert S3 URI to a public HTTPS URL"""
    bucket, key = s3_url_parse(s3_uri)
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def get_last_modified(file_path: str):
    """Returns the Last-Modified timestamp
    of a given URL if available."""
    if is_gcsfs_path(file_path):
        url = file_path.replace("gs://", "https://storage.googleapis.com/")
    elif is_s3_path(file_path):
        url = s3_uri_to_public_url(file_path)
    else:
        url = file_path

    assert is_url(url)
    response = requests.head(url, allow_redirects=True)
    last_modified = response.headers.get("Last-Modified")
    if last_modified:
        return parsedate_to_datetime(last_modified)
    else:
        return None
