import io
import json
import os
from typing import Text, Dict, Any

from jinja2 import Environment, FileSystemLoader

BLOCK_PREFIX = "@block:"
EXPR_PREFIX = "@expr:"
RAW_STRING_PREFIX = "@raw:"
