from typing import Generator, List, Dict, Optional

from databricks_terraformer.hcl import RAW_STRING_PREFIX
from databricks_terraformer.sdk.export import APIGenerator
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.service.policies_service import PolicyService
from databricks_terraformer.sdk.utils import normalize_identifier


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
            policy_id = policy['policy_id']
            policy_name = policy['name']
            if self._match_patterns(policy_name) is False:
                continue
            identifier = normalize_identifier(f"databricks_cluster_policy-{policy_name}-{policy_id}")
            json = {
                "definition": policy["definition"],
                "name": policy_name
            }
            yield APIData(policy_id, self.api_client.url,
                          identifier, json, self.get_local_hcl_path(identifier))

    @property
    def resource_name(self) -> str:
        return "databricks_cluster_policy"

    @property
    def _resource_var_dot_paths(self) -> List[str]:
        return []

    @property
    def _map_var_dot_path_dict(self) -> Optional[Dict[str, Optional[str]]]:
        return
