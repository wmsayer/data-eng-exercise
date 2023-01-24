import pandas as pd
from shroomdk import ShroomDK
import scripts.SnowflakeAPI as snwflk
import os
from sys import platform
from dotenv import load_dotenv
import pathlib

PROJECT_ROOT = "%s" % pathlib.Path(__file__).parent.parent.parent.parent.absolute()


if platform == "linux":
    load_dotenv('/home/ubuntu/.bashrc')

API_KEY = os.environ.get('FLIPSIDE_API_KEY')
SNWFLK_DB = os.environ.get('SNOWFLAKE_DB')

# default params for low-level API
TTL_MINUTES = 15
PAGE_SIZE = 100000  # return up to 100,000 results per GET request on the query id
PAGE_NUMBER = 1  # return results of page 1


class Flipside:
    def __init__(self, snwflk_db=SNWFLK_DB):
        self.api_key = API_KEY
        self.query = ""
        self.df = pd.DataFrame()

        # Initialize `ShroomDK` with the Flipside API Key
        self.sdk = ShroomDK(self.api_key)

        # Snowflake params
        self.snwflk_db = snwflk_db

        self.log_book = {
            "addr_bals": {"fn": self.get_eth_addr_bals, "freq": 5*60}
        }

    def run_query(self, query):
        self.query = query

        # print("Running Flipside query...")
        self.run_flipside_query_sdk()
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

    def get_eth_addr_bals(self, write_snwflk=True):
        addr_list_path = "/".join([PROJECT_ROOT, "/seeds/network_address_labels.csv"])
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
            snwflk_api = snwflk.SnowflakeAPI(schema=snwflk_schema, db=self.snwflk_db)
            snwflk_api.write_df(self.df, snwflk_table, replace=True)

        return self.df


if __name__ == "__main__":
    test_api = Flipside()
    test_df = test_api.get_eth_addr_bals(write_snwflk=True)
    print(test_df)
    # test_output_path = "/data/flipside_test.csv"
    # test_df.to_csv(test_output_path, index=False)
