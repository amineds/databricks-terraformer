import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.hcl.json_to_hcl import create_hcl_from_json
from databricks_terraformer.utils.git_handler import GitHandler
from databricks_terraformer.utils.names import gen_valid_name
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export DBFS files.")
@click.option("--hcl", is_flag=True, help='Will export the data as HCL.')
@provide_pattern_func("pattern_matches")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def export_cli(api_client: ApiClient, hcl, pattern_matches):
    if hcl:
        log.debug("this if debug")
        pool_api = InstancePoolsApi(api_client)

        pools = pool_api.list_instance_pools()["instance_pools"]
        log.info(pools)

        #with GitHandler("git@github.com:itaiw/export-repo.git", "instance_pools", ignore_deletes=True) as gh:
        for pool in pools:
            assert "instance_pool_name" in pool
            assert "instance_pool_id" in pool
            assert "state" in pool

            base_name = gen_valid_name(pool["instance_pool_name"])

            pool_resource_data = {
                "instance_pool_name" : pool["instance_pool_name"],
                "min_idle_instances" : pool["min_idle_instances"],
                "node_type_id" : pool["node_type_id"],
                "@block:aws_attributes" : {
                    "availability" : pool["aws_attributes"]["availability"],
                    "zone_id" : pool["aws_attributes"]["zone_id"],
                    "spot_bid_price_percent" : pool["aws_attributes"]["spot_bid_price_percent"],
                },
                "idle_instance_autotermination_minutes" : pool["idle_instance_autotermination_minutes"],
                "disk_spec" : {
                    "ebs_volume_type" : pool["disk_spc"]["GENERAL_PURPOSE_SSD"],
                    "disk_size" : pool["disk_spc"]["80"],
                    "disk_count" : pool["disk_spc"]["1"],
                },
                "custom_tags" : {
                    "creator" : "Sriharsha Tikkireddy",
                    "testChange" : "Sri Tikkireddy",
                },
                # "source": f'${{pathexpand("{pool["path"]}")}}',
                # "content_b64_md5": f'${{md5(filebase64(pathexpand("{pool["path"]}"}}',
                # "path": pool["path"],
                # "overwrite": True,
                # "mkdirs": True,
                # "validate_remote_file": True,
            }
            att_exists(pool_resource_data, pool,"max_capacity")
            block_exists(pool_resource_data, pool,"disk_spec")

            o_type = "resource"
            name = "databricks_instance_pool"
            identifier = f"databricks_instance_pool-{base_name}"
            policy_hcl = create_hcl_from_json(o_type, name, identifier, pool_resource_data, False)

            print(policy_hcl)


def att_exists(pool_resource_data, pool,att):
    if att in pool:
        pool_resource_data[att]= pool[att]

def block_exists(pool_resource_data, pool,block):
    if block in pool:
        pool_resource_data[block] = pool[att]

@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Instance Pools.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def instance_pools_group():
    """
    Utility to interact with Databricks Instance Pools.
    """
    pass


instance_pools_group.add_command(export_cli, name="export")

#GIT_PYTHON_TRACE=full databricks-terraformer -v debug instance-pools export --hcl --profile demo