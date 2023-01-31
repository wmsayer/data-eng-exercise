import json
import requests
import numpy as np
import logging
from http.client import HTTPConnection # py3

line_break = "/"*69


def debug_requests_on():
    '''Switches on logging of the requests module.'''
    HTTPConnection.debuglevel = 1

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def json_load(f_path):
    """Loads a local JSON file as a Python dict"""
    with open(f_path) as f:
        json_data = json.load(f)
    return json_data


def run_rest_get(url, params={}, headers={}, print_summ=True, print_resp=False):

    if print_summ:
        print(line_break)
        print("GET Address: %s\nHeaders %s:\nParameters %s:" % (url, repr(headers), repr(params)))

    response = requests.get(url, params=params, headers=headers)
    status_code = response.status_code

    if status_code == 200:
        json_dict = response.json()
    else:
        json_dict = {}

    if print_summ:
        print("Status Code: %d" % response.status_code)
        # print("Message: %d" % response.messa)
        if type(json_dict) == dict:
            print("Response Keys: %s\n" % json_dict.keys())
    if print_resp:
        print("Response: %s\n" % json_dict)

    return json_dict, status_code


def format_num_to_sig_figs(val, sig_figs=3, prefix="$", suffix=""):
    """Assumes single value, not array."""
    power = np.floor(np.log10(val))

    if power >= 0:
        unit_i = np.floor(power/3)
        unit_lab = {0: "", 1: "k", 2: "M", 3: "B", 4: "T"}[unit_i]
        new_val = val / 10 ** (unit_i*3)

    else:
        unit_lab = ""
        new_val = val

    new_val_str = prefix + ('{0:.%sg}' % sig_figs).format(new_val) + unit_lab + suffix
    # print(f"OG val: {val} --- New val: {new_val_str}")
    return new_val_str


if __name__ == "__main__":
    for i in range(-4, 11):
        format_num_to_sig_figs(11**i, sig_figs=3)
