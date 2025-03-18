"""
STAC to EO3 translation
Adapted from
https://github.com/opendatacube/odc-tools/blob/develop/apps/dc_tools/odc/apps/dc_tools/_stac.py

See https://github.com/opendatacube/odc-tools/issues/615
"""

from typing import Any, Dict

from odc.apps.dc_tools._docs import odc_uuid
from odc.apps.dc_tools._stac import (
    _check_valid_uuid,
    _geographic_to_projected,
    _get_stac_bands,
    _get_stac_properties_lineage,
    _stac_product_lookup,
)
from odc.geo.geom import Geometry, box
from toolz import get_in

Document = Dict[str, Any]


def stac_transform(input_stac: Document) -> Document:
    """Takes in a raw STAC 1.0 dictionary and returns an ODC dictionary"""
    # pylint: disable=too-many-locals

    (
        dataset_id,
        dataset_label,
        product_name,
        region_code,
        default_grid,
    ) = _stac_product_lookup(input_stac)

    # Generating UUID for products not having UUID.
    # Checking if provided id is valid UUID.
    # If not valid, creating new deterministic uuid using odc_uuid function
    # based on product_name and product_label.
    # TODO: Verify if this approach to create UUID is valid.
    if _check_valid_uuid(input_stac["id"]):
        deterministic_uuid = input_stac["id"]
    else:
        if product_name in ["s2_l2a"]:
            deterministic_uuid = str(odc_uuid("sentinel-2_stac_process", "1.0.0", [dataset_id]))
        else:
            deterministic_uuid = str(
                odc_uuid(f"{product_name}_stac_process", "1.0.0", [dataset_id])
            )

    # Check for projection extension properties that are not in the asset fields.
    # Specifically, proj:shape and proj:transform, as these are otherwise
    # fetched in _get_stac_bands.
    properties = input_stac["properties"]
    proj_shape = properties.get("proj:shape")
    proj_transform = properties.get("proj:transform")
    # TODO: handle old STAC that doesn't have grid information here...
    bands, grids, accessories = _get_stac_bands(
        input_stac,
        default_grid,
        proj_shape=proj_shape,
        proj_transform=proj_transform,
    )

    # STAC document may not have top-level proj:shape property
    # use one of the bands as a default
    proj_shape = grids.get("default").get("shape")
    proj_transform = grids.get("default").get("transform")

    stac_properties, lineage = _get_stac_properties_lineage(input_stac)

    # Support https://stac-extensions.github.io/projection/v2.0.0/schema.json
    try:
        # Expected format "proj:code": "EPSG:3857"
        epsg = properties["proj:code"]
        native_crs = properties["proj:code"].lower()
    except KeyError:
        # v1.0.0 to v1.2.0 expected format "proj:epsg": 3857
        epsg = properties["proj:epsg"]
        native_crs = f"epsg:{epsg}"

    # Transform geometry to the native CRS at an appropriate precision
    geometry = Geometry(input_stac["geometry"], "epsg:4326")
    if native_crs != "epsg:4326":
        # Arbitrary precisions, but should be fine
        pixel_size = get_in(["default", "transform", 0], grids, no_default=True)
        precision = 0
        if pixel_size < 0:
            precision = 6

        geometry = _geographic_to_projected(geometry, native_crs, precision)

    if geometry is not None:
        # We have a geometry, but let's make it simple
        geom_type = None
        try:
            geom_type = geometry.geom_type
        except AttributeError:
            geom_type = geometry.type
        if geom_type is not None and geom_type == "MultiPolygon":
            geometry = geometry.convex_hull
    else:
        # Build geometry from the native transform
        min_x = proj_transform[2]
        min_y = proj_transform[5]
        max_x = min_x + proj_transform[0] * proj_shape[0]
        max_y = min_y + proj_transform[4] * proj_shape[1]

        if min_y > max_y:
            min_y, max_y = max_y, min_y

        geometry = box(min_x, min_y, max_x, max_y, native_crs)

    stac_odc = {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "id": deterministic_uuid,
        "crs": native_crs,
        "grids": grids,
        "product": {"name": product_name.lower()},
        "properties": stac_properties,
        "measurements": bands,
        "lineage": {},
        "accessories": accessories,
    }
    if dataset_label:
        stac_odc["label"] = dataset_label

    if region_code:
        stac_odc["properties"]["odc:region_code"] = region_code

    if geometry:
        stac_odc["geometry"] = geometry.json

    if lineage:
        stac_odc["lineage"] = lineage

    return stac_odc
