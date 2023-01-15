import pandas as pd
# from shroomdk import ShroomDK
import json
import pathlib
import requests
import time


def json_load(f_path):
    """Loads a local JSON file as a Python dict"""
    f = open(f_path, )
    r_data = json.load(f)
    f.close()
    return r_data


# def get_flipside_data(query):
#     # In practice, load credentials as a Kubernetes secret!
#     api_key_path = "/".join(["%s" % pathlib.Path(__file__).parent.absolute(), "flipside_api_key.json"])
#     api_key = json_load(api_key_path)["api_key"]
#
#     # Initialize `ShroomDK` with the Flipside API Key
#     sdk = ShroomDK(api_key)
#
#     # Run the query against Flipside's query engine
#     query_result_set = sdk.query(query)
#
#     # convert result to pandas df
#     df = pd.json_normalize(query_result_set.records)
#
#     # clean dataset
#     df = df.loc[df["has_price"], :]
#     df = df.loc[df["has_decimal"], :]
#     df.sort_values(by="symbol", inplace=True)
#
#     return df

# def model(dbt, session):
#     # my_sql_model_df = dbt.ref("my_sql_model")
#
#     # build Flipside SQL query string
#     model_address = "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0"  # Kraken 4
#     query = f"""
#                         SELECT
#                             *
#                         FROM ethereum.core.ez_current_balances
#                         WHERE user_address = LOWER('{model_address}')
#                     """
#     result_df = get_flipside_data(query)

api_key_path = "/".join(["%s" % pathlib.Path(__file__).parent.absolute(), "flipside_api_key.json"])
API_KEY = json_load(api_key_path)["api_key"]

TTL_MINUTES = 15

# return up to 100,000 results per GET request on the query id
PAGE_SIZE = 100000

# return results of page 1
PAGE_NUMBER = 1


def create_query(sql_query):
    r = requests.post(
        'https://node-api.flipsidecrypto.com/queries',
        data=json.dumps({
            "sql": sql_query,
            "ttlMinutes": TTL_MINUTES
        }),
        headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY},
    )
    if r.status_code != 200:
        raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))

    return json.loads(r.text)


def get_query_results(token):
    r = requests.get(
        'https://node-api.flipsidecrypto.com/queries/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
            token=token,
            page_number=PAGE_NUMBER,
            page_size=PAGE_SIZE
        ),
        headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY}
    )
    if r.status_code != 200:
        raise Exception(
            "Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))

    data = json.loads(r.text)
    if data['status'] == 'running':
        time.sleep(10)
        return get_query_results(token)

    result_df = pd.DataFrame(data['results'])
    result_df.columns = data['columnLabels']

    return result_df


def run_flipside_query(sql_query):
    query = create_query(sql_query)
    token = query.get('token')
    result_df = get_query_results(token)
    return result_df


def get_eth_addr_bals():
    # build Flipside SQL query string
    model_address = "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0"  # Kraken 4
    sql_query = f"""
                                SELECT
                                    *
                                FROM ethereum.core.ez_current_balances
                                WHERE user_address = LOWER('{model_address}')
                            """
    result_df = run_flipside_query(sql_query)
    return result_df


def model(dbt, session):
    # my_sql_model_df = dbt.ref("my_sql_model")
    result_df = get_eth_addr_bals()
    return result_df


if __name__ == "__main__":
    test_df = get_eth_addr_bals()
    print(test_df)
    test_output_path = "C:/Users/wsaye/PycharmProjects/data-eng-exercise/data/flipside_test.csv"
    test_df.to_csv(test_output_path, index=False)
