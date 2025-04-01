import click

from external_odc_products_py.tools import s3_to_dc_v2


@click.group(name="indexing-tools", help="Toos to help indexing datasets")
def indexing_tools():
    pass


indexing_tools.add_command(s3_to_dc_v2.cli)
