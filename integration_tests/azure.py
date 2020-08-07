from databricks_cli.configure.provider import get_config_for_profile
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.workspace.api import  WorkspaceApi
from databricks_terraformer.cluster_policies.policies_service import PolicyService
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk import ApiClient


profile = 'dr_source'
config = get_config_for_profile(profile)
api_client = ApiClient(host=config.host, token=config.token)

policy_json = {"name":"test_policy", "definition":"{}"}

cluster_json = {
                    "cluster_name": "no pool std cluster 1",
                    "spark_version": "6.5.x-scala2.11",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "node_type_id": "Standard_DS3_v2",
                    "driver_node_type_id": "Standard_DS3_v2",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "autotermination_minutes": 120,
                    "init_scripts": [
                        {
                            "dbfs": {
                                "destination": "dbfs:/does_not_exists.sh"
                            }
                        }
                    ],
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }

instance_pool_json_list = [
        {
            "instance_pool_name": "small instance pool 2",
            "min_idle_instances": 0,
            "max_capacity": 5,
            "node_type_id": "Standard_DS3_v2",
            "custom_tags": {
                "custom": "1"
            },
            "idle_instance_autotermination_minutes": 120,
            "enable_elastic_disk": True,
            "preloaded_spark_versions": [
                "7.1.x-scala2.12"
            ]
        },
        {
            "instance_pool_name": "medium instance pool 2",
            "min_idle_instances": 0,
            "max_capacity": 5,
            "node_type_id": "Standard_DS5_v2",
            "custom_tags": {
                "pool": "Medium"
            },
            "idle_instance_autotermination_minutes": 60,
            "enable_elastic_disk": True,
            "preloaded_spark_versions": [
                "7.1.x-scala2.12"
            ]
        }]

cluster_policy_json_list=[
        {
            "name": "second policy",
            "definition": "{\n  \"instance_pool_id\": { \"type\": \"forbidden\" }\n}"
        },
        {
            "name": "first policy",
            "definition": "{\n  \"spark_version\": {\n    \"type\": \"regex\",\n    \"pattern\": \"7\\\\.[0-9]+\\\\.x-scala.*\"\n  },\n  \"dbus_per_hour\": {\n    \"type\": \"range\",\n    \"maxValue\": 10\n  },\n  \"instance_pool_id\": {\n    \"type\": \"forbidden\",\n    \"hidden\": true\n  },\n  \"spark_conf.spark.databricks.io.cache.enabled\": {\n    \"type\": \"fixed\",\n    \"value\": \"true\"\n  },\n  \"autotermination_minutes\": {\n    \"type\": \"fixed\",\n    \"value\": 20,\n    \"hidden\": true\n  }\n}"
        }
    ]


def create_clusters():
    print("Creating clusters")
    cluster_api = ClusterApi(api_client)
    cluster_id = cluster_api.create_cluster(cluster_json)
    print(cluster_id)
    cluster_api.delete_cluster(cluster_id["cluster_id"])
    cluster_json["autotermination_minutes"] = 60
    cluster_json["cluster_name"] = "no pool std cluster 2"
    cluster_id = cluster_api.create_cluster(cluster_json)
    cluster_api.delete_cluster(cluster_id["cluster_id"])


def create_pools():
    print("Creating pools")
    pool_api = InstancePoolsApi(api_client)
    for pool in instance_pool_json_list:
        pool_api.create_instance_pool(pool)

def create_policies():
    print("Creating policies")
    service = PolicyService(api_client)
    for policy in cluster_policy_json_list:
        service.create_policy(policy["name"],policy["definition"])

def upload_dbfs_file():
    print("Upload files to DBFS")
    dbfs_api = DbfsApi(api_client)
    dbfs_api.put_file("example_notebook.py", DbfsPath("dbfs:/example_notebook.py"), True)

def upload_notebook():
    print("Upload notebooks")
    workspace_api = WorkspaceApi(api_client)
    workspace_api.import_workspace("example_notebook.py", "/Shared/example_notebook", "PYTHON", "SOURCE", True)

def cleanup():
    cluster_api = ClusterApi(api_client)
    cluster_list = cluster_api.list_clusters()["clusters"]

    for cluster in cluster_list:
        cluster_api.delete_cluster(cluster["cluster_id"])

    pool_api = InstancePoolsApi(api_client)
    pool_list = pool_api.list_instance_pools()
    if "instance_pools" in pool_list:
        for pool in pool_list["instance_pools"]:
            pool_api.delete_instance_pool(pool["instance_pool_id"])

    service = PolicyService(api_client)
    policy_list = service.list_policies()
    if "policies" in policy_list:
        for policy in policy_list["policies"]:
            service.delete_policy(policy["policy_id"])

    dbfs_api = DbfsApi(api_client)
    dbfs_api.delete(DbfsPath("dbfs:/example_notebook.py"), False)

    workspace_api = WorkspaceApi(api_client)
    workspace_api.delete("/Shared/example_notebook", False)

#create_clusters()
#create_pools()
#create_policies()
#upload_dbfs_file()
#upload_notebook()
cleanup()

