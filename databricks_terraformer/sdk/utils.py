import abc
import functools
import re
from typing import Text, Dict, Any


class DictDotPathVisitor(abc.ABC):
    @abc.abstractmethod
    def visit(self, d: Dict[Text, Any], key: Text):
        pass


class RekeyVisitor(DictDotPathVisitor):
    def __init__(self, new_key):
        self.new_key = new_key

    def visit(self, d, key):
        d[self.new_key] = pop_value_annotated(key, d)


class SetValueVisitor(DictDotPathVisitor):
    def __init__(self, new_value):
        self.new_value = new_value

    def visit(self, d, key):
        d[get_key_annotated(key, d)] = self.new_value


class GetValueVisitor(DictDotPathVisitor):
    def __init__(self):
        self.__values = []

    def visit(self, d, key):
        v = get_value_annotated(key, d)
        self.__values.append(v)

    @property
    def values(self):
        return self.__values


def pop_value_annotated(k: str, d: Dict[Text, Any]):
    keys = list(d.keys())
    unannotated_keys = [key.split(":")[-1] for key in keys]
    for idx, key in enumerate(unannotated_keys):
        if k == key:
            return d.pop(keys[idx])
    raise KeyError(f"key: {k} not found in {d}")


def get_key_annotated(k: str, d: Dict[Text, Any]):
    keys = list(d.keys())
    unannotated_keys = [key.split(":")[-1] for key in keys]
    for idx, key in enumerate(unannotated_keys):
        if k == key:
            return keys[idx]
    raise KeyError(f"key: {k} not found in {d}")


def get_value_annotated(k: str, d: Dict[Text, Any]):
    keys = list(d.keys())
    unannotated_keys = [key.split(":")[-1] for key in keys]
    for idx, key in enumerate(unannotated_keys):
        if k == key:
            return d[keys[idx]]
    raise KeyError(f"key: {k} not found in {d}")


def is_array(key: str) -> bool:
    return key.startswith("[") and key.endswith("]")


def get_array_value(key):
    return key.replace("[", "").replace("]", "")


def walk_via_dot(old_key_dot: Text, d: Dict[Text, Any],
                 *visitors: DictDotPathVisitor) -> Any:
    keys = old_key_dot.split(".")
    last_key = keys[-1]
    if len(keys) > 1:
        for idx, key in enumerate(keys[:-1]):
            if is_array(key) and type(d) == list:
                if get_array_value(key) == "*":
                    for list_item in d:
                        walk_via_dot(".".join(keys[idx + 1:]), list_item, *visitors)
                elif int(get_array_value(key)) < len(d):
                    walk_via_dot(".".join(keys[idx + 1:]), d[int(get_array_value(key))], *visitors)
                else:
                    raise IndexError(f"cannot look for {get_array_value(key)} in list {d}")
                return
            if key not in d.keys() and key not in [key.split(":")[-1] for key in d.keys()]:
                raise KeyError(f"key: {key} in {old_key_dot} not found in {d}")
            d = get_value_annotated(key, d)

    for visitor in visitors:
        visitor.visit(d, last_key)


def normalize(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        output = func(*args, **kwargs)
        return normalize_identifier(output)

    return wrapper


def normalize_identifier(identifier):
    return_name = remove_emoji(identifier)
    if identifier[0].isdigit():
        return_name = "_" + identifier

    return re.sub("[^a-zA-Z0-9_]+", "_", return_name)


def remove_emoji(text):
    regrex_pattern = re.compile("["
                                u"\U0001F600-\U0001F64F"  # emoticons
                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                u"\U00002500-\U00002BEF"  # chinese char
                                u"\U00002702-\U000027B0"
                                u"\U00002702-\U000027B0"
                                u"\U000024C2-\U0001F251"
                                u"\U0001f926-\U0001f937"
                                u"\U00010000-\U0010ffff"
                                u"\u2640-\u2642"
                                u"\u2600-\u2B55"
                                u"\u200d"
                                u"\u23cf"
                                u"\u23e9"
                                u"\u231a"
                                u"\ufe0f"  # dingbats
                                u"\u3030"
                                "]+", flags=re.UNICODE)

    return regrex_pattern.sub(r'', text)
