from pathlib import Path
from typing import Callable, Dict

from databricks_terraformer.sdk.generators import ResourceCatalog
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder, Block, Expression
from databricks_terraformer.sdk.message import APIData, HCLConvertData
from databricks_terraformer.sdk.service.permissions import PermissionService
from databricks_terraformer.sdk.utils import normalize


class NoDirectPermissionsError(ValueError):
    pass


class PermissionsHelper:
    class TerraformPermissionType:
        def __init__(self, object_type, object_id_name, resource_id_field):
            self.resource_id_attribute = resource_id_field
            self.object_id_name = object_id_name
            self.object_type = object_type

    def __init__(self, api_client):
        self.api_client = api_client
        self._permissions_service = PermissionService(self.api_client)

        self.perm_mapping: Dict[str, 'TerraformPermissionType'] = {
            ResourceCatalog.NOTEBOOK_RESOURCE: self.TerraformPermissionType("notebooks", "notebook_id", "object_id"),
            ResourceCatalog.CLUSTER_POLICY_RESOURCE: self.TerraformPermissionType("cluster-policies",
                                                                                  "cluster_policy_id", "id")
        }

    @staticmethod
    @normalize
    def _make_identifier(raw_id: str) -> str:
        return f"databricks_permissions-{raw_id}"

    def _create_permission_dictionary(self, src_obj_data: HCLConvertData, permission_acls):
        permission_list = []
        for item in permission_acls:
            for perm in item["all_permissions"]:
                if perm["inherited"]:
                    continue
                data = {"group_name": item.get("group_name", ""), "user_name": item.get("user_name", ""),
                        "permission_level": perm["permission_level"]}
                permission_list.append(data)
        if len(permission_list) == 0:
            raise NoDirectPermissionsError("cannot have no acls that are directly attributed")
        perms = self.perm_mapping[src_obj_data.resource_name]
        tdb = TerraformDictBuilder()
        tdb. \
            add_required(perms.object_id_name,
                         lambda: f"{src_obj_data.resource_name}.{src_obj_data.hcl_resource_identifier}."
                                 f"{perms.resource_id_attribute}",
                         Expression()). \
            add_required("access_control", lambda: permission_list, Block())
        return tdb.to_dict()

    def create_permission_data(self,
                               src_obj_data: HCLConvertData,
                               path_func: Callable[[str], Path]):
        perm_data = self._permissions_service.get_object_permissions(
            self.perm_mapping[src_obj_data.resource_name].object_type, src_obj_data.raw_id)
        identifier = self._make_identifier(src_obj_data.raw_id)
        api_data = APIData(
            src_obj_data.raw_id,
            self.api_client.url,
            identifier,
            self._create_permission_dictionary(src_obj_data,
                                               perm_data["access_control_list"]),
            path_func(identifier))
        return HCLConvertData(ResourceCatalog.PERMISSIONS_RESOURCE, api_data,
                              processors=[])
