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
