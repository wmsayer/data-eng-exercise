import pandas as pd
import scripts.Admin as Admin
import snowflake
from snowflake.connector.pandas_tools import write_pandas, pd_writer
import streamlit as st

CNNX_PATH = "C:/Users/wsaye/Desktop/private_api/snowflake.json"
DEFAULT_PROFILE = "chip"
DEFAULT_WH = "TRANSFORMING"


class SnowflakeAPI:
    def __init__(self, db, schema, profile="chip", wh=DEFAULT_WH):
        self.profile = profile
        self.db = db
        self.schema = schema
        self.wh = wh
        self.cnnx_params = self.get_cnnx_params()

    def get_cnnx_params(self):
        if self.profile == "streamlit":
            cnnx_params = st.secrets
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
        print("Running Snowflake query...")
        cnnx = self.get_cnnx()
        cursor = cnnx.cursor()
        cursor.execute(sql_query)
        cursor.close()
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

        print("Writing to Snowflake...")
        success, nchunks, nrows, output = write_pandas(cnnx, df, table)
        print(f"\tSnowflake success: {success}\n\tChunks: {nchunks}\n\tRows: {nrows}")

    def write_df_ll(self, df, table, mode="replace"):
        """Possible modes are 'fail', 'replace', 'append'."""
        cnnx = self.get_cnnx()
        num_rows = df.to_sql(table, cnnx, if_exists=mode, index=False, method=pd_writer)
        print(num_rows)


if __name__ == "__main__":
    test_api = SnowflakeAPI(db="flipside", schema="dbt_wsayer2")
    test_cnnx = test_api.get_cnnx()
    test_query = """
    SELECT entity, SUM(usd_value)
    FROM curr_entity_bals
    WHERE usd_value > 1000
        AND entity IN ('Coinbase', 'Kraken', 'Binance')
    GROUP BY 1
    """
    test_df = test_api.run_get_query(test_query)
    print(test_df)
