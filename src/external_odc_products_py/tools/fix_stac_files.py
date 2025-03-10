# Fix the proj:code property in the generated stac files into proj:epsg
import json
import os
import re

from odc.aws import s3_dump
from tqdm import tqdm

from external_odc_products_py.io import (
    check_file_extension,
    get_filesystem,
    is_gcsfs_path,
    is_s3_path,
)
from external_odc_products_py.utils import fix_stac_item

# stac_files_dir = "data/esa_worldcereal_sample/wintercereals/tc-wintercereals/"
# stac_files = find_json_files(os.path.abspath(stac_files_dir))


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


stac_files_dir = "s3://deafrica-data-dev-af/esa_worldcereal_sample/wintercereals/tc-wintercereals"
stac_files = find_json_files(stac_files_dir)


with tqdm(iterable=stac_files, desc="Fix stac files", total=len(stac_files)) as files:
    for file_path in files:
        fs = get_filesystem(file_path, anon=True)
        with fs.open(file_path, "r") as f:
            stac_item = json.load(f)
            stac_item = fix_stac_item(stac_item)

        # Write stac item
        if is_s3_path(file_path):
            s3_dump(
                data=json.dumps(stac_item, indent=2),
                url=file_path,
                ACL="bucket-owner-full-control",
                ContentType="application/json",
            )
        else:
            with open(file_path, "w") as f:
                json.dump(stac_item, f, indent=2)  # `indent=4` makes it human-readable
