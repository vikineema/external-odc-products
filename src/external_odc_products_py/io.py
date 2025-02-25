import logging
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import fsspec
import gcsfs
import s3fs
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from s3fs.core import S3FileSystem

from external_odc_products_py.logs import get_logger

logger = get_logger(Path(__file__).stem, level=logging.INFO)


def is_s3_path(path: str) -> bool:
    o = urlparse(path)
    if o.scheme in ["s3"]:
        return True
    else:
        return False


def is_gcsfs_path(path: str) -> bool:
    o = urlparse(path)
    if o.scheme in ["gcs", "gs"]:
        return True
    else:
        return False


def is_url(path: str) -> bool:
    o = urlparse(path)
    if o.scheme in ["http", "https"]:
        return True
    else:
        return False


def get_filesystem(
    path: str,
    anon: bool = True,
) -> S3FileSystem | LocalFileSystem | GCSFileSystem:
    if is_s3_path(path=path):
        fs = s3fs.S3FileSystem(anon=anon, s3_additional_kwargs={"ACL": "bucket-owner-full-control"})
    elif is_gcsfs_path(path=path):
        if anon:
            fs = gcsfs.GCSFileSystem(token="anon")
        else:
            fs = gcsfs.GCSFileSystem()
    else:
        fs = fsspec.filesystem("file")
    return fs


def check_file_exists(path: str) -> bool:
    fs = get_filesystem(path=path, anon=True)
    if fs.exists(path) and fs.isfile(path):
        return True
    else:
        return False


def check_directory_exists(path: str) -> bool:
    fs = get_filesystem(path=path, anon=True)
    if fs.exists(path) and fs.isdir(path):
        return True
    else:
        return False


def check_file_extension(path: str, accepted_file_extensions: list[str]) -> bool:
    _, file_extension = os.path.splitext(path)
    if file_extension.lower() in accepted_file_extensions:
        return True
    else:
        return False


def is_geotiff(path: str) -> bool:
    accepted_geotiff_extensions = [".tif", ".tiff", ".gtiff"]
    return check_file_extension(path=path, accepted_file_extensions=accepted_geotiff_extensions)


def find_geotiff_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    file_name_pattern = re.compile(file_name_pattern)

    fs = get_filesystem(path=directory_path, anon=True)

    geotiff_file_paths = []

    for root, dirs, files in fs.walk(directory_path):
        for file_name in files:
            if is_geotiff(path=file_name):
                if re.search(file_name_pattern, file_name):
                    geotiff_file_paths.append(os.path.join(root, file_name))
                else:
                    continue
            else:
                continue

    if is_s3_path(path=directory_path):
        geotiff_file_paths = [f"s3://{file}" for file in geotiff_file_paths]
    elif is_gcsfs_path(path=directory_path):
        geotiff_file_paths = [f"gs://{file}" for file in geotiff_file_paths]
    return geotiff_file_paths


def is_json(path: str) -> bool:
    accepted_json_extensions = [".json"]
    return check_file_extension(path=path, accepted_file_extensions=accepted_json_extensions)


def find_json_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    file_name_pattern = re.compile(file_name_pattern)

    fs = get_filesystem(path=directory_path, anon=True)

    json_file_paths = []

    for root, dirs, files in fs.walk(directory_path):
        for file_name in files:
            if is_json(path=file_name):
                if re.search(file_name_pattern, file_name):
                    json_file_paths.append(os.path.join(root, file_name))
                else:
                    continue
            else:
                continue

    if is_s3_path(path=directory_path):
        json_file_paths = [f"s3://{file}" for file in json_file_paths]
    elif is_gcsfs_path(path=directory_path):
        json_file_paths = [f"gs://{file}" for file in json_file_paths]
    return json_file_paths
