import click

from external_odc_products_py.iwmi_odr.create_stac import create_stac_files


@click.group(name="iwmi-odr", help="Run tools for ESA World Cereal Products.")
def iwmi_odr():
    pass


iwmi_odr.add_command(create_stac_files)
