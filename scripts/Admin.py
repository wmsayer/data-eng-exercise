import json


def json_load(f_path):
    """Loads a local JSON file as a Python dict"""
    with open(f_path) as f:
        # f = open(f_path, )
        json_data = json.load(f)
        # f.close()
    return json_data
