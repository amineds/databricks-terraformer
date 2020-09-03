import io
from base64 import b64decode
from pathlib import Path
from typing import Generator, List, Dict, Optional, Any

from databricks_cli.dbfs.api import FileInfo, BUFFER_SIZE_BYTES
from databricks_cli.sdk import DbfsService, ApiClient
from databricks_cli.utils import error_and_quit

from databricks_terraformer.sdk.hcl import EXPR_PREFIX
from databricks_terraformer.sdk.pipeline import DownloaderAPIGenerator
from databricks_terraformer.sdk.message import APIData, Artifact


class DbfsFile(Artifact):

    @staticmethod
    def __get_file_contents(dbfs_service: DbfsService, dbfs_path: str, headers=None):
        abs_path = f"dbfs:{dbfs_path}"
        json = dbfs_service.get_status(abs_path, headers=headers)
        file_info = FileInfo.from_json(json)
        if file_info.is_dir:
            error_and_quit('The dbfs file {} is a directory.'.format(repr(abs_path)))
        length = file_info.file_size
        offset = 0
        output = io.StringIO()
        while offset < length:
            response = dbfs_service.read(abs_path, offset, BUFFER_SIZE_BYTES,
                                         headers=headers)
            bytes_read = response['bytes_read']
            data = response['data']
            offset += bytes_read
            output.write(b64decode(data).decode("utf-8"))
        return output.getvalue()

    def get_content(self):
        return DbfsFile.__get_file_contents(self.service, self.remote_path)


class DbfsFileHCLGenerator(DownloaderAPIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, dbfs_path: str, patterns=None,
                 custom_dynamic_vars=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns,
                         custom_map_vars=custom_map_vars,
                         custom_dynamic_vars=custom_dynamic_vars)
        self.__dbfs_path = dbfs_path
        self.__service = DbfsService(self.api_client)

    @property
    def resource_name(self) -> str:
        return "databricks_dbfs_file"

    @property
    def _annotation_dot_paths(self) -> Dict[str, List[str]]:
        return {
            EXPR_PREFIX: ["source", "content_b64_md5"]
        }

    @property
    def _resource_var_dot_paths(self) -> List[str]:
        return []

    @property
    def _map_var_dot_path_dict(self) -> Optional[Dict[str, Optional[str]]]:
        return None

    @staticmethod
    def __get_dbfs_file_data_recrusive(service: DbfsService, path):
        resp = service.list(path)
        if "files" not in resp:
            return []
        files = resp["files"]
        for file in files:
            if file["is_dir"] is True:
                yield from DbfsFileHCLGenerator.__get_dbfs_file_data_recrusive(service, file["path"])
            else:
                yield file

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        return [DbfsFile(remote_path=data["path"],
                         local_path=self.get_local_download_path(self.get_identifier(data)),
                         service=self.__service)]

    def _define_identifier(self, data: Dict[str, Any]) -> str:
        return f"{self.resource_name}-{data['path']}"

    def get_raw_id(self, data: Dict[str, Any]) -> str:
        return data["path"]

    def make_hcl_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "source": f'pathexpand("{self.get_identifier(data)}")',
            "content_b64_md5": f'md5(filebase64(pathexpand("{self.get_identifier(data)}")))',
            "path": data["path"],
            "overwrite": True,
            "mkdirs": True,
            "validate_remote_file": True,
        }

    async def _generate(self) -> Generator[APIData, None, None]:
        service = DbfsService(self.api_client)
        for file in DbfsFileHCLGenerator.__get_dbfs_file_data_recrusive(service, self.__dbfs_path):
            yield file
