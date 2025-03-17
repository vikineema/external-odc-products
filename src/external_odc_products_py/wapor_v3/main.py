import click

from external_odc_products_py.wapor_v3.create_stac import create_stac_files
from external_odc_products_py.wapor_v3.download_cogs import download_cogs


@click.group(name="wapor-v3", help="Run tools for WaPOR version 3 Products.")
def wapor_v3():
    pass


wapor_v3.add_command(create_stac_files)
wapor_v3.add_command(download_cogs)
