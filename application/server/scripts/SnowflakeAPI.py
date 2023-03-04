import pandas as pd
import snowflake
from snowflake.connector.pandas_tools import write_pandas
import logging
import os
from sys import platform
from dotenv import load_dotenv
import pathlib
import time


# to supress logging messages
for name in logging.Logger.manager.loggerDict.keys():
    if 'snowflake' in name:
        logging.getLogger(name).setLevel(logging.WARNING)
        logging.getLogger(name).propagate = False

if platform == "linux":
    load_dotenv('/home/ubuntu/.bashrc')


SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PWD = os.environ.get('SNOWFLAKE_PWD')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
DEFAULT_DB = os.environ.get('SNOWFLAKE_DB')
DEFAULT_WH = "COMPUTE_WH"


class SnowflakeAPI:
    def __init__(self, schema, db=DEFAULT_DB, wh=DEFAULT_WH, print_summ=False):
        self.db = db
        self.schema = schema
        self.wh = wh
        self.print_summ = print_summ

    def get_cnnx(self):
        cnnx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PWD,
            account=SNOWFLAKE_ACCOUNT,
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

    def write_df(self, df, table,  replace=False, print_summ=True):
        """This method appends by default."""

        cnnx = self.get_cnnx()

        if replace:
            drop_query = f"DELETE FROM {self.db}.{self.schema}.{table}"
            self.run_query(drop_query)

        df.columns = [c.upper() for c in df.columns]
        success, nchunks, nrows, output = write_pandas(cnnx, df, table)

        if print_summ:
            logging.info(f"\tSnowflake success: {success}, Chunks: {nchunks}, Rows: {nrows}")

    def init_snwflk_trending(self):
        local_path = "/".join([PROJECT_ROOT, "data/cg_historical_trending_log.csv"])
        existing_df = pd.read_csv(local_path)
        existing_df.columns = [c.upper() for c in existing_df.columns]

        # snwflk_api = snwflk.SnowflakeAPI(schema='COINGECKO', db="BIGDORKSONLY")
        self.write_df(existing_df, table='TRENDING_LOG', replace=True)

    def backup_table_local(self, table_name="TRENDING_LOG"):
        query = f"SELECT * FROM {self.db}.{self.schema}.{table_name}"
        backup_df = self.run_get_query(query)

        write_fn = f"data/backups/{self.db}_{self.schema}_{table_name}_backup_{round(time.time())}.csv"
        write_path = "/".join([PROJECT_ROOT, write_fn])
        backup_df.to_csv(write_path, index=False)
        print(backup_df)


if __name__ == "__main__":
    PROJECT_ROOT = "%s" % pathlib.Path(__file__).parent.parent.parent.parent.absolute()
    test_backup_api = SnowflakeAPI(db="BIGDORKSONLY", schema="COINGECKO")
    test_backup_api.backup_table_local()

    # test_api = SnowflakeAPI(db="bigdorksonly", schema="dbt_output_dev")
    # test_cnnx = test_api.get_cnnx()
    # test_query = """
    # SELECT *
    # FROM bigdorksonly.coingecko.historical_prices
    # """
    # test_df = test_api.run_get_query(test_query)
    # print(test_df)
