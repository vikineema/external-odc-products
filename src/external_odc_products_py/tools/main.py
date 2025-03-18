import click

from external_odc_products_py.tools import fs_to_dc, s3_to_dc


@click.group(name="indexing-tools", help="Toos to help indexing datasets")
def indexing_tools():
    pass


indexing_tools.add_command(s3_to_dc.cli)
indexing_tools.add_command(fs_to_dc.cli)
