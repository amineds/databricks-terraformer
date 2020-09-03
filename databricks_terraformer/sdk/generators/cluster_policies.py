from typing import Generator, List, Dict, Optional, Any

from databricks_terraformer.sdk.hcl import RAW_STRING_PREFIX
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.service.cluster_policies import PolicyService


class ClusterPolicyHCLGenerator(APIGenerator):

    @property
    def _annotation_dot_paths(self) -> Dict[str, List[str]]:
        return {
            RAW_STRING_PREFIX: ["definition"]
        }

    async def _generate(self) -> Generator[APIData, None, None]:
        service = PolicyService(self.api_client)
        policies = service.list_policies()["policies"]
        for policy in policies:
            yield policy

    @property
    def resource_name(self) -> str:
        return "databricks_cluster_policy"

    @property
    def _resource_var_dot_paths(self) -> List[str]:
        return []

    @property
    def _map_var_dot_path_dict(self) -> Optional[Dict[str, Optional[str]]]:
        return

    def _define_identifier(self, data: Dict[str, Any]) -> str:
        return f"databricks_cluster_policy-{data['name']}-{self.get_raw_id(data)}"

    def get_raw_id(self, data: Dict[str, Any]) -> str:
        return data['policy_id']

    def make_hcl_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "definition": data["definition"],
            "name": data["name"]
        }
