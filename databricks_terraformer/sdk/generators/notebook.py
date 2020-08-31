from base64 import b64decode
from pathlib import Path
from typing import List, Generator, Dict, Optional

from databricks_cli.sdk import WorkspaceService, ApiClient
from databricks_cli.workspace.api import WorkspaceFileInfo

from databricks_terraformer import log
from databricks_terraformer.hcl import EXPR_PREFIX
from databricks_terraformer.sdk.export import DownloaderAPIGenerator
from databricks_terraformer.sdk.message import Artifact, APIData
from databricks_terraformer.sdk.utils import normalize_identifier


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
        print(f"Fetched all files & folders from path: {path}")
        if "objects" not in resp:
            return []
        objects = resp["objects"]
        for obj in objects:
            workspace_obj = WorkspaceFileInfo.from_json(obj)
            if workspace_obj.is_notebook is True:
                yield workspace_obj
            if workspace_obj.is_dir is True:
                yield from NotebookHCLGenerator._get_notebooks_recursive(service, workspace_obj.path)

    async def _generate(self) -> Generator[APIData, None, None]:
        service = WorkspaceService(self.api_client)
        for file in NotebookHCLGenerator._get_notebooks_recursive(service, self.__notebook_path):
            if self._match_patterns(file.path) is False:
                continue
            identifier = normalize_identifier(f"{self.resource_name}-{file.path}")
            json = {
                "content": f'filebase64("{identifier}")',
                "path": file.path,
                "overwrite": True,
                "mkdirs": True,
                "language": file.language,
                "format": "SOURCE",
            }
            required_artifact = NotebookArtifact(remote_path=file.path,
                                                 local_path=self.get_local_download_path(identifier),
                                                 service=service)
            yield APIData(file.path, self.api_client.url,
                          identifier, json, self.get_local_hcl_path(identifier), artifacts=[required_artifact])
