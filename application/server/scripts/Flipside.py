import pandas as pd
import application.server.scripts.Admin as Admin
import application.server.scripts.SnowflakeAPI as snwflk
from shroomdk import ShroomDK
import json
import requests
import time

api_key_path = "C:/Users/wsaye/Desktop/private_api/flipside.json"
API_KEY = Admin.json_load(api_key_path)["api_key"]

# default params for low-level API
TTL_MINUTES = 15
PAGE_SIZE = 100000  # return up to 100,000 results per GET request on the query id
PAGE_NUMBER = 1  # return results of page 1


class Flipside:
    def __init__(self, use_sdk=True):
        self.api_key = API_KEY
        self.query = ""
        self.df = pd.DataFrame()
        self.use_sdk = use_sdk

        if self.use_sdk:
            # Initialize `ShroomDK` with the Flipside API Key
            self.sdk = ShroomDK(self.api_key)

        # Snowflake params
        self.snwflk_db = "FLIPSIDE"
        self.snwflk_prof = "chip"

        self.log_book = {
            "addr_bals": {"fn": self.get_eth_addr_bals, "freq": 5*60}
        }

    def run_query(self, query):
        self.query = query

        # print("Running Flipside query...")
        if self.use_sdk:
            self.run_flipside_query_sdk()
        else:
            self.run_flipside_query_ll()
        # print("\tFlipside query complete.")

        self.df.columns = [c.upper() for c in self.df.columns]  # required all caps for Snowflake

        return self.df

    def run_flipside_query_sdk(self):
        """Run Flipside query using ShroomDK."""
        if self.query:
            # Run the query against Flipside's query engine
            query_result_set = self.sdk.query(self.query)

            # convert result to pandas df
            self.df = pd.json_normalize(query_result_set.records)

    def run_flipside_query_ll(self):
        """Run Flipside query using low level method."""
        query = self.create_query()
        token = query.get('token')
        self.df = self.get_query_results(token)

    def create_query(self):
        print(json.dumps({
                "sql": self.query,
                "ttlMinutes": TTL_MINUTES
            }))
        r = requests.post(
            'https://node-api.flipsidecrypto.com/queries',
            data=json.dumps({
                "sql": self.query,
                "ttlMinutes": TTL_MINUTES
            }),
            headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": self.api_key},
        )
        if r.status_code != 200:
            raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))

        return json.loads(r.text)

    def get_query_results(self, token):
        r = requests.get(
            'https://node-api.flipsidecrypto.com/queries/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
                token=token,
                page_number=PAGE_NUMBER,
                page_size=PAGE_SIZE
            ),
            headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": self.api_key}
        )
        if r.status_code != 200:
            raise Exception(
                "Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))

        data = json.loads(r.text)
        if data['status'] == 'running':
            time.sleep(10)
            return self.get_query_results(token)

        result_df = pd.DataFrame(data['results'])
        result_df.columns = data['columnLabels']

        return result_df

    def get_eth_addr_bals(self, write_snwflk=True):
        addr_list_path = "/seeds/network_address_labels.csv"
        addr_list_df = pd.read_csv(addr_list_path)
        addr_list = list(addr_list_df["address"].values)
        addr_list_sql_str = ", ".join([f"LOWER('{a}')" for a in addr_list])
        sql_query = f"""
                        SELECT *
                        FROM ethereum.core.ez_current_balances
                        WHERE user_address IN ({addr_list_sql_str})
                    """

        self.run_query(sql_query)

        # clean dataset
        self.df = self.df.loc[self.df["HAS_PRICE"], :]
        self.df = self.df.loc[self.df["HAS_DECIMAL"], :]
        self.df["CURRENT_BAL_UNADJ"] = self.df["CURRENT_BAL_UNADJ"].astype(str)
        self.df["NETWORK"] = "Ethereum"
        self.df.sort_values(by="SYMBOL", inplace=True)

        if write_snwflk:
            snwflk_schema = 'ACCOUNT_BALS'
            snwflk_table = 'CURR_ETH_BALS'
            snwflk_api = snwflk.SnowflakeAPI(db=self.snwflk_db, schema=snwflk_schema, profile=self.snwflk_prof)
            snwflk_api.write_df(self.df, snwflk_table, replace=True)

        return self.df


if __name__ == "__main__":
    test_api = Flipside(use_sdk=True)
    test_df = test_api.get_eth_addr_bals(write_snwflk=True)
    print(test_df)
    test_output_path = "/data/flipside_test.csv"
    test_df.to_csv(test_output_path, index=False)
