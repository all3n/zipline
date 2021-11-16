import yaml
import re
import os

VAR_PATTERN = re.compile(r'\$\{([\w]+)\}')

def load_yaml(yaml_file,raise_error = True):
    yaml_dir = os.path.dirname(os.path.abspath(yaml_file))
    out = {}
    if os.path.exists(yaml_file):
        with open(yaml_file, "rb") as f_yaml:
            yaml_info = yaml.load(f_yaml, Loader=yaml.FullLoader)
            if yaml_info and 'includes' in yaml_info:
                for include in yaml_info['includes']:
                    sub_yaml = load_yaml(os.path.join(yaml_dir, include), raise_error)
                    out.update(sub_yaml)
                out.update(yaml_info)
            else:
                out = yaml_info
            return out
    else:
        if raise_error:
            raise RuntimeError(yaml_file + " not exists")
        else:
            return {}
