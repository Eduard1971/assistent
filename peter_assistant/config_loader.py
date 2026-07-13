from __future__ import annotations
import os,re,yaml
from pathlib import Path

def _expand(value):
    if isinstance(value,str):
        return re.sub(r"\$\{([^}]+)\}",lambda m:os.getenv(m.group(1),""),value)
    if isinstance(value,list): return [_expand(x) for x in value]
    if isinstance(value,dict): return {k:_expand(v) for k,v in value.items()}
    return value

def load_config(path:str):
    return _expand(yaml.safe_load(Path(path).read_text(encoding="utf-8")))
