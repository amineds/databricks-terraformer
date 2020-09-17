import abc
import functools
import json
import traceback
from pathlib import Path
from typing import List, Optional, Any

from databricks_terraformer import log
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformJsonBuilder, \
    TerraformDictBuilder


class Artifact(abc.ABC):
    def __init__(self, remote_path, local_path: Path, service):
        self.local_path = local_path
        self.remote_path = remote_path
        self.service = service

    @abc.abstractmethod
    def get_content(self):
        pass


class APIData:

    def __init__(self, raw_identifier, workspace_url,
                 hcl_resource_identifier, data, local_save_path: Path, artifacts: Optional[List[Any]] = None):
        self.__workspace_url = workspace_url
        self.__raw_identifier = raw_identifier
        self.__local_save_path = local_save_path
        self.__data = data
        self.__hcl_resource_identifier = hcl_resource_identifier
        self.__artifacts: List[Artifact] = artifacts or []

    @property
    def artifacts(self) -> List[Artifact]:
        return self.__artifacts

    @property
    def workspace_url(self):
        return self.__workspace_url

    @property
    def raw_identifier(self):
        return self.__raw_identifier

    @property
    def hcl_resource_identifier(self):
        return self.__hcl_resource_identifier

    @property
    def data(self):
        return self.__data

    @property
    def local_save_path(self):
        return self.__local_save_path


class Variable:
    def __init__(self, variable_name, default):
        self.default = default
        self.variable_name = variable_name

    def __eq__(self, obj):
        return isinstance(obj, Variable) and obj.__dict__ == self.__dict__

    def __repr__(self):
        return json.dumps(self.__dict__)

    def to_dict(self):
        if self.default is None:
            return {}
        else:
            return {
                "default": self.default
            }

    def to_hcl(self, debug: bool):
        tdb = TerraformDictBuilder(). \
            add_optional_if(lambda: self.default is not None, "default", lambda: self.default)
        return TerraformJsonBuilder(). \
            add_variable(self.variable_name, tdb.to_dict()). \
            to_json()


class ErrorMixin:
    def __init__(self):
        self.__errors = []

    def add_error(self, error):
        self.__errors.append(error)

    @property
    def errors(self):
        return self.__errors

    @staticmethod
    def manage_error(func) -> Any:

        @functools.wraps(func)
        def wrapper(inp: ErrorMixin):
            if not isinstance(inp, ErrorMixin):
                return func(inp)

            if len(inp.errors) > 0:
                # This is if the function gets called and an error already exists
                log.info("Found error when processing function: " + func.__name__)
                log.info("Error List: " + str(inp.errors))
                return inp
            try:
                resp = func(inp)
                return resp
            except Exception as e:
                traceback.print_exc()
                inp.add_error(e)
                return inp

        wrapper.managed_error = True
        return wrapper


class HCLConvertData(ErrorMixin):

    def __init__(self, resource_name, raw_api_data: APIData, processors: List['Processor'] = None):
        super().__init__()
        self.__raw_api_data = raw_api_data
        self.__resource_name = resource_name
        self.__processors = processors or []
        self.__lineage = []
        self.__lineage.append(raw_api_data.data)
        self.__mapped_variables = []
        self.__resource_variables = []

    @property
    def raw_id(self):
        return self.__raw_api_data.raw_identifier

    @property
    def local_save_path(self):
        return self.__raw_api_data.local_save_path

    @property
    def resource_name(self):
        return self.__resource_name

    @property
    def artifacts(self) -> List[Artifact]:
        return self.__raw_api_data.artifacts

    @property
    def processors(self):
        return self.__processors

    @property
    def hcl_resource_identifier(self):
        return self.__raw_api_data.hcl_resource_identifier

    @property
    def latest_version(self):
        return self.__lineage[-1]

    @property
    def lineage(self):
        return self.__lineage

    @property
    def mapped_variables(self) -> List[Variable]:
        return self.__mapped_variables

    @property
    def resource_variables(self) -> List[Variable]:
        return self.__resource_variables

    def modify_json(self, value):
        self.__lineage.append(value)

    def add_mapped_variable(self, variable_name, variable_default_value):
        self.__mapped_variables.append(Variable(variable_name, variable_default_value))

    def add_resource_variable(self, variable_name, variable_default_value):
        self.__resource_variables.append(Variable(variable_name, variable_default_value))

    def to_hcl(self, debug: bool):
        tjb = TerraformJsonBuilder()
        for r_var in self.resource_variables:
            tjb.add_variable(r_var.variable_name, r_var.to_dict())
        tjb.add_resource(self.resource_name, self.hcl_resource_identifier, self.latest_version)
        return tjb.to_json()
        # variable_hcls = "\n".join([r_var.to_hcl(debug) for r_var in self.resource_variables])
        # resource_hcl = create_resource_from_dict(self.resource_name, self.hcl_resource_identifier,
        #                                          self.latest_version,
        #                                          debug)
        # hcl_code = "\n".join([variable_hcls, resource_hcl])
        # return create_hcl_file(self.hcl_resource_identifier, self.__raw_api_data.workspace_url, self.latest_version,
        #                        hcl_code)
