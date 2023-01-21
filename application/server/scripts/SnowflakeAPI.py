import pandas as pd
import application.server.scripts.Admin as Admin
import snowflake
from snowflake.connector.pandas_tools import write_pandas
import logging
import os

# to supress logging messages
for name in logging.Logger.manager.loggerDict.keys():
    if 'snowflake' in name:
        logging.getLogger(name).setLevel(logging.WARNING)
        logging.getLogger(name).propagate = False

CNNX_PATH = "C:/Users/wsaye/Desktop/private_api/snowflake.json"
DEFAULT_PROFILE = "chip"
DEFAULT_WH = "TRANSFORMING"


class SnowflakeAPI:
    def __init__(self, db, schema, profile="chip", wh=DEFAULT_WH, print_summ=False):
        self.profile = profile
        self.db = db
        self.schema = schema
        self.wh = wh
        self.cnnx_params = self.get_cnnx_params()
        self.print_summ = print_summ

    def get_cnnx_params(self):
        if self.profile == "server":
            cnnx_params = {
                "SNOWFLAKE_USER": os.getenv('SNOWFLAKE_USER'),
                "SNOWFLAKE_PWD": os.getenv('SNOWFLAKE_PWD'),
                "SNOWFLAKE_ACCOUNT": os.getenv('SNOWFLAKE_ACCOUNT'),
            }
        else:
            cnnx_params = Admin.json_load(CNNX_PATH)[self.profile]
        return cnnx_params

    def get_cnnx(self):
        cnnx = snowflake.connector.connect(
            user=self.cnnx_params["SNOWFLAKE_USER"],
            password=self.cnnx_params["SNOWFLAKE_PWD"],
            account=self.cnnx_params["SNOWFLAKE_ACCOUNT"],
            database=self.db,
            schema=self.schema,
            warehouse=self.wh
        )
        return cnnx

    def run_query(self, sql_query):
        if self.print_summ:
            print("Running Snowflake query...")

        cnnx = self.get_cnnx()
        cursor = cnnx.cursor()
        cursor.execute(sql_query)
        cursor.close()

        if self.print_summ:
            print("\tSnowflake query complete.")

    def run_get_query(self, sql_query):
        results_df = pd.read_sql(sql_query, con=self.get_cnnx())
        return results_df

    def write_df(self, df, table,  replace=False):
        """This method appends by default."""

        cnnx = self.get_cnnx()

        if replace:
            drop_query = f"DELETE FROM {self.db}.{self.schema}.{table}"
            self.run_query(drop_query)

        if self.print_summ:
            print("Writing to Snowflake...")

        success, nchunks, nrows, output = write_pandas(cnnx, df, table)

        if self.print_summ:
            print(f"\tSnowflake success: {success}, Chunks: {nchunks}, Rows: {nrows}")


if __name__ == "__main__":
    test_api = SnowflakeAPI(db="flipside", schema="dbt_wsayer2")
    test_cnnx = test_api.get_cnnx()
    test_query = """
    SELECT *
    FROM flipside.coingecko.historical_prices
    """
    test_df = test_api.run_get_query(test_query)
    print(test_df)
