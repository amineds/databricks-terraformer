from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient

from databricks_terraformer.sdk.generators import ResourceCatalog
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.service.scim import ScimService


class ScimUsersHCLGenerator(APIGenerator):
    @property
    def folder_name(self) -> str:
        return "scim_user"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars
        self.__service = ScimService(ApiClient)

    def __get_user_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data,
                                   lambda d: f"databricks_scim_user-{d['userName']}-{self.__get_user_raw_id(d)}")

    def __get_user_raw_id(self, data: Dict[str, Any]) -> str:
        return data['id']

    def __user_is_admin(self, data: Dict[str, Any]):
        group_is_admin_lst = [True if group["display"] == "admins" else False for group in data.get("groups", [])]
        return lambda: any(group_is_admin_lst)

    def __make_user_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("user_name", lambda: data["userName"]). \
            add_required("default_roles", lambda: []). \
            add_optional("display_name", lambda: data["displayName"]). \
            add_optional("roles", lambda: [valuePair["value"] for valuePair in data["roles"]]). \
            add_optional("entitlements", lambda: [valuePair["value"] for valuePair in data["entitlements"]]). \
            add_optional_if(self.__user_is_admin(data), "set_admin", lambda: True). \
            to_dict()

    def __create_scim_user_data(self, user_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.SCIM_USER_RESOURCE,
            user_data,
            lambda: any([self._match_patterns(user_data["userName"])]) is False,
            self.__get_user_identifier,
            self.__get_user_raw_id,
            self.__make_user_dict,
            self.map_processors(self.__custom_map_vars)
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        service = ScimService(self.api_client)
        users = service.list_users()["Resources"]
        for user in users:
            yield self.__create_scim_user_data(user)
