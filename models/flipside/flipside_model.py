import pandas as pd
from shroomdk import ShroomDK
import json
import pathlib


def json_load(f_path):
    """Loads a local JSON file as a Python dict"""
    f = open(f_path, )
    r_data = json.load(f)
    f.close()
    return r_data


def get_flipside_data(query):
    # In practice, load credentials as a Kubernetes secret!
    api_key_path = "/".join(["%s" % pathlib.Path(__file__).parent.absolute(), "flipside_api_key.json"])
    api_key = json_load(api_key_path)["api_key"]

    # Initialize `ShroomDK` with the Flipside API Key
    sdk = ShroomDK(api_key)

    # Run the query against Flipside's query engine
    query_result_set = sdk.query(query)

    # convert result to pandas df
    df = pd.json_normalize(query_result_set.records)

    # clean dataset
    df = df.loc[df["has_price"], :]
    df = df.loc[df["has_decimal"], :]
    df.sort_values(by="symbol", inplace=True)

    return df


def model(dbt, session):
    # my_sql_model_df = dbt.ref("my_sql_model")

    # build Flipside SQL query string
    model_address = "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0"  # Kraken 4
    query = f"""
                        SELECT
                            *
                        FROM ethereum.core.ez_current_balances
                        WHERE user_address = LOWER('{model_address}')
                    """
    result_df = get_flipside_data(query)

    return result_df


if __name__ == "__main__":
    test_address = "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0"  # Kraken 4
    test_query = f"""
                        SELECT
                            *
                        FROM ethereum.core.ez_current_balances
                        WHERE user_address = LOWER('{test_address}')
                    """
    test_df = get_flipside_data(test_query)
    test_output_path = "/data/flipside_test.csv"
    test_df.to_csv(test_output_path, index=False)
