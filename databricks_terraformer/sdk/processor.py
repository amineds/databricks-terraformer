import abc
import copy
from typing import List, Text, Dict, Optional

from pygrok import Grok

from databricks_terraformer.sdk.hcl import EXPR_PREFIX, RAW_STRING_PREFIX
from databricks_terraformer.sdk.message import HCLConvertData
from databricks_terraformer.sdk.utils import RekeyVisitor, walk_via_dot, normalize_identifier, \
    SetValueVisitor, GetValueVisitor


class Processor(abc.ABC):
    """
    The goal of the processor is to provide a function ot modify the json returned from api calls into
    a more hcl looking format to send over to the golang shared lib functions to generate HCL content.
    """

    def process(self, terraform_model: 'HCLConvertData'):
        self._process(terraform_model)

    @abc.abstractmethod
    def _process(self, terraform_model: 'HCLConvertData'):
        pass


class BasicAnnotationProcessor(Processor):
    """
    The BasicAnnotationProcessor applies a given prefix to the BLOCK_PREFIX, EXPR_PREFIX or RAW_PREFIX to the
    appropriate given key in the dot path.
    """

    def __init__(self, prefix: str, dot_paths: List[Text] = None):
        self._dot_paths = dot_paths
        self._prefix = prefix

    def _annotate(self, d) -> None:
        for key in self._dot_paths:
            last_value = key.split(".")[-1]
            new_key = self._prefix + last_value
            visitor = RekeyVisitor(new_key)
            # TODO: there may need to be a debug statement for this to ignore the error
            #  but log the fact we were unable to find key in the msg
            walk_via_dot(key, d, visitor)

    def _process(self, terraform_model: HCLConvertData):
        this_dict = copy.deepcopy(terraform_model.latest_version)
        self._annotate(this_dict)
        terraform_model.modify_json(this_dict)


def verify_no_nested_prefix(dot_paths: List[str]):
    dot_path_set = list(set(dot_paths))
    for i in dot_path_set:
        for j in dot_path_set:
            if i == j:
                continue
            if i in j:
                raise ValueError(f"cannot have a value {i} that is a prefix of {j}")


class ResourceVariableBasicAnnotationProcessor(BasicAnnotationProcessor):
    """
    The ResourceVariableBasicAnnotationProcessor converts the given field in the HCL interpolated by a variable and
    exposes it within the HCL document as a variable object.
    """

    def __init__(self, resource_name: str, dot_paths: List[Text] = None):
        super().__init__(EXPR_PREFIX, dot_paths)
        self.resource_name = resource_name
        verify_no_nested_prefix(dot_paths)

    def __get_resource_variable_name(self, identifier, key):
        return f"{self.resource_name}_{identifier}_{normalize_identifier(key)}"

    def _process(self, terraform_model: HCLConvertData):
        for resource_var_dot_path in self._dot_paths:
            this_key = resource_var_dot_path.split(".")[-1]
            variable_name = self.__get_resource_variable_name(terraform_model.hcl_resource_identifier, this_key)
            visitor = GetValueVisitor()
            # TODO: there may need to be a debug statement for this to ignore the error
            #  but log the fact we were unable to find key in the msg
            walk_via_dot(resource_var_dot_path, terraform_model.latest_version, visitor)
            set_value = visitor.values[0]
            terraform_model.add_resource_variable(variable_name, set_value)
            visitor = SetValueVisitor(f"var.{variable_name}")
            walk_via_dot(resource_var_dot_path, terraform_model.latest_version, visitor)
            self._annotate(terraform_model.latest_version)


class MappedVariableBasicAnnotationProcessor(BasicAnnotationProcessor):
    """
    The MappedVariableBasicAnnotationProcessor converts the given value in the HCL interpolated by a variable and
    exposes it globally in the application as a global variable. This may be things like instance_type, etc that you
    may want to map to other values across environments.
    """

    def __init__(self, resource_name: str, dot_paths: List[Text] = None):
        super().__init__(EXPR_PREFIX, dot_paths)
        self.resource_name = resource_name
        verify_no_nested_prefix(dot_paths)

    def _process(self, terraform_model: HCLConvertData):
        for map_var_dot_path in self._dot_paths:
            visitor = GetValueVisitor()
            walk_via_dot(map_var_dot_path, terraform_model.latest_version, visitor)
            value = visitor.values[0]
            variable_name = f"{normalize_identifier(value)}"
            terraform_model.add_mapped_variable(variable_name, value)
            visitor = SetValueVisitor(f"var.{variable_name}")
            # TODO: there may need to be a debug statement for this to ignore the error
            #  but log the fact we were unable to find key in the msg
            walk_via_dot(map_var_dot_path, terraform_model.latest_version, visitor)
            self._annotate(terraform_model.latest_version)


class MappedGrokVariableBasicAnnotationProcessor(BasicAnnotationProcessor):
    """
    The MappedGrokVariableBasicAnnotationProcessor converts the given value in the HCL interpolated by a variable and
    exposes it globally in the application as a global variable. This may be things like instance_type, etc that you
    may want to map to other values across environments.
    """

    def __init__(self, resource_name: str, dot_path_grok_dict: Dict[Text, Text] = None):
        super().__init__(RAW_STRING_PREFIX, list(dot_path_grok_dict.keys()))
        self.resource_name = resource_name
        self.__dot_path_grok_dict = dot_path_grok_dict

    def __value_is_interpolated(self, val: str) -> bool:
        return val.startswith("${var.") and val.endswith("}")

    def __sub_grok(self, key, value) -> Optional[str]:
        pattern = self.__dot_path_grok_dict[key] or "%{GREEDYDATA:value}"
        grok = Grok(pattern)
        res = grok.match(value)
        if res is None:
            return None
        if res is not None and len(list(res.keys())) > 1:
            return None
        for _, groked_val in res.items():
            return groked_val

    def __wrap_interpolation(self, value):
        return "${var." + value + "}"

    def __get_resource_value(self, orig_value, groked_value, variable):
        parameter_wrapped = self.__wrap_interpolation(variable)
        return orig_value.replace(groked_value, parameter_wrapped)

    def _process(self, terraform_model: HCLConvertData):
        for map_var_dot_path in self._dot_paths:
            visitor = GetValueVisitor()
            walk_via_dot(map_var_dot_path, terraform_model.latest_version, visitor)
            raw_value = visitor.values[0]
            # TODO: retry to see if there are multiple instances
            final_lines = []
            for line in raw_value.split("\n"):
                groked_value = self.__sub_grok(map_var_dot_path, line)
                if groked_value is None:
                    final_lines.append(line)
                    continue
                variable_name = f"{normalize_identifier(groked_value)}"
                final_value = self.__get_resource_value(line, groked_value, variable_name)
                terraform_model.add_mapped_variable(variable_name, groked_value)
                final_lines.append(final_value)
            visitor = SetValueVisitor("\n".join(final_lines))
            # TODO: there may need to be a debug statement for this to ignore the error
            #  but log the fact we were unable to find key in the msg
            walk_via_dot(map_var_dot_path, terraform_model.latest_version, visitor)

            self._annotate(terraform_model.latest_version)
