import click as click
import click_log

from databricks_terraformer import CONTEXT_SETTINGS, log
# from databricks_terraformer.apply.cli import destroy_cli, import_cli
from databricks_terraformer.cmds.export import export_cli
from databricks_terraformer.cmds.version import print_version_callback, version


@click.group(CONTEXT_SETTINGS)
@click.option('--version', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@click_log.simple_verbosity_option(log, '--verbosity', '-v')
def cli():
    pass


cli.add_command(export_cli, name="export")
# TODO: refactor apply
# cli.add_command(import_cli, name="import")
# cli.add_command(destroy_cli, name="destroy")


if __name__ == "__main__":
    cli()