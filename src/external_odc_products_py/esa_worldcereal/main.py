import click

from external_odc_products_py.esa_worldcereal.create_stac import create_stac_files
from external_odc_products_py.esa_worldcereal.download_cogs import download_cogs


@click.group(name="esa-worldcereal", help="Run tools for ESA World Cereal Products.")
def esa_worldcereal():
    pass


esa_worldcereal.add_command(create_stac_files)
esa_worldcereal.add_command(download_cogs)
