import json
import os
from dotenv import load_dotenv

from databricks_cli.configure.provider import get_config_for_profile
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.workspace.api import WorkspaceApi
from databricks_terraformer.cluster_policies.policies_service import PolicyService
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk import ApiClient

# setup
path = os.path.dirname(__file__)
print(path)
dotenv_path = os.path.join(path,'../.env')
print(dotenv_path)
load_dotenv(dotenv_path=dotenv_path)

src_profile = os.environ.get("AZURE_SOURCE_WORKSPACE")
print(src_profile)
config = get_config_for_profile(src_profile)

# api setup
api_client = ApiClient(host=config.host, token=config.token)
cluster_api = ClusterApi(api_client)
pool_api = InstancePoolsApi(api_client)
dbfs_api = DbfsApi(api_client)
workspace_api = WorkspaceApi(api_client)
policy_service = PolicyService(api_client)

# read objects json
with open(os.path.join(path, "clusters.json")) as jsonfile:
    clusters_json = json.load(jsonfile)

with open(os.path.join(path, "instance_pools.json")) as jsonfile:
    instance_pools_json_list = json.load(jsonfile)

with open(os.path.join(path, "cluster_policies.json")) as jsonfile:
    cluster_policies_json_list = json.load(jsonfile)


def create_clusters():
    print("Creating clusters")
    for cluster in clusters_json:
        cluster_api.create_cluster(cluster)

    # as we "manually" create a new cluster, we need to update the cleanup list as well
    clusters_json[0]["autotermination_minutes"] = 60
    clusters_json[0]["cluster_name"] = "no pool std cluster 2"
    cluster_api.create_cluster(clusters_json[0])


def delete_clusters():
    print("Deleting clusters")
    cluster_list = cluster_api.list_clusters()
    test_clusters = []
    for cluster in clusters_json:
        test_clusters.append(cluster["cluster_name"])
    # add our manually created cluster
    test_clusters.append("no pool std cluster 2")
    if "clusters" in cluster_list:
        for cluster in cluster_list["clusters"]:
            if cluster["cluster_name"] in test_clusters:
                cluster_api.delete_cluster(cluster["cluster_id"])


def create_pools():
    print("Creating pools")
    for pool in instance_pools_json_list:
        pool_api.create_instance_pool(pool)


def delete_pools():
    print("Deleting pools")
    pool_list = pool_api.list_instance_pools()
    test_pools = []
    for pool in instance_pools_json_list:
        test_pools.append(pool["instance_pool_name"])
    if "instance_pools" in pool_list:
        for pool in pool_list["instance_pools"]:
            if pool["instance_pool_name"] in test_pools:
                pool_api.delete_instance_pool(pool["instance_pool_id"])


def create_policies():
    print("Creating policies")
    policy_service = PolicyService(api_client)
    for policy in cluster_policies_json_list:
        policy_service.create_policy(policy["name"], policy["definition"])


def delete_policies():
    print("Deleting policies")
    policy_list = policy_service.list_policies()
    test_policies = []
    for policy in cluster_policies_json_list:
        test_policies.append(policy["name"])
    if "policies" in policy_list:
        for policy in policy_list["policies"]:
            if policy["name"] in test_policies:
                policy_service.delete_policy(policy["policy_id"])


def upload_dbfs_file():
    print("Upload files to DBFS")
    dbfs_api.put_file(os.path.join(path, "example_notebook.py"), DbfsPath("dbfs:/example_notebook.py"), True)


def create_group_instance_profile():
    # SCIM API is still in preview and is not reflected in the CLI
    pass


def remove_dbfs_file():
    print("Removing DBFS files")
    try:
        file_exists = dbfs_api.get_status(DbfsPath("dbfs:/example_notebook.py"))
        dbfs_api.delete(DbfsPath("dbfs:/example_notebook.py"), False)
    except:
        pass


def upload_notebook():
    print("Upload notebooks")
    workspace_api.import_workspace(os.path.join(path, "example_notebook.py"), "/Shared/example_notebook", "PYTHON",
                                   "SOURCE", True)


def remove_notebook():
    print("Removing notebooks")
    try:
        notebook_exists = workspace_api.list_objects("/Shared/example_notebook")
        workspace_api.delete("/Shared/example_notebook", False)
    except:
        pass


delete_clusters()
delete_pools()
delete_policies()
remove_dbfs_file()
remove_notebook()

create_clusters()
create_pools()
create_policies()
upload_dbfs_file()
upload_notebook()
