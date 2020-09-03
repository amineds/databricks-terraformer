from base64 import b64decode
from pathlib import Path
from typing import List, Generator, Dict, Optional, Any

from databricks_cli.sdk import WorkspaceService, ApiClient
from databricks_cli.workspace.api import WorkspaceFileInfo

from databricks_terraformer import log
from databricks_terraformer.sdk.hcl import EXPR_PREFIX
from databricks_terraformer.sdk.pipeline import DownloaderAPIGenerator
from databricks_terraformer.sdk.message import Artifact, APIData


class NotebookArtifact(Artifact):

    def get_content(self):
        data = self.service.export_workspace(self.remote_path, format="SOURCE")
        if "content" not in data:
            log.error(f"Unable to find content for file {self.remote_path}")
            raise FileNotFoundError(f"Unable to find content for notebook in {self.remote_path}")
        return b64decode(data["content"].encode("utf-8")).decode("utf-8")


class NotebookHCLGenerator(DownloaderAPIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, notebook_path: str, patterns=None,
                 custom_dynamic_vars=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns,
                         custom_map_vars=custom_map_vars,
                         custom_dynamic_vars=custom_dynamic_vars)
        self.__notebook_path = notebook_path
        self.__service = WorkspaceService(self.api_client)

    @property
    def _annotation_dot_paths(self) -> Dict[str, List[str]]:
        return {
            EXPR_PREFIX: ["content"]
        }

    @property
    def _resource_var_dot_paths(self) -> List[str]:
        return []

    @property
    def _map_var_dot_path_dict(self) -> Optional[Dict[str, Optional[str]]]:
        return {"format": None}

    @property
    def resource_name(self) -> str:
        return "databricks_notebook"

    @staticmethod
    def _get_notebooks_recursive(service: WorkspaceService, path: str):
        resp = service.list(path)
        log.info(f"Fetched all files & folders from path: {path}")
        if "objects" not in resp:
            return []
        objects = resp["objects"]
        for obj in objects:
            workspace_obj = WorkspaceFileInfo.from_json(obj)
            if workspace_obj.is_notebook is True:
                yield workspace_obj.__dict__
            if workspace_obj.is_dir is True:
                yield from NotebookHCLGenerator._get_notebooks_recursive(service, workspace_obj.path)

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        return [NotebookArtifact(remote_path=data['path'],
                                 local_path=self.get_local_download_path(self.get_identifier(data)),
                                 service=self.__service)]

    def _define_identifier(self, data: Dict[str, Any]) -> str:
        return f"{self.resource_name}-{data['path']}"

    def get_raw_id(self, data: Dict[str, Any]) -> str:
        return data['path']

    def make_hcl_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
                "content": f'filebase64("{self.get_identifier(data)}")',
                "path": data['path'],
                "overwrite": True,
                "mkdirs": True,
                "language": data['language'],
                "format": "SOURCE",
            }

    async def _generate(self) -> Generator[APIData, None, None]:
        service = WorkspaceService(self.api_client)
        for item in NotebookHCLGenerator._get_notebooks_recursive(service, self.__notebook_path):
            yield item
