import scripts.Admin as Admin
import snowflake
from snowflake.connector.pandas_tools import write_pandas, pd_writer

CNNX_PATH = "C:/Users/wsaye/Desktop/private_api/snowflake.json"
CNNX_PROFILES = Admin.json_load(CNNX_PATH)
DEFAULT_PROFILE = "chip"
DEFAULT_WH = "TRANSFORMING"


class SnowflakeAPI:
    def __init__(self, db, schema, profile="chip"):
        self.profile = profile
        self.db = db
        self.schema = schema
        self.wh =DEFAULT_WH
        self.cnnx_params = CNNX_PROFILES[self.profile]

    def get_cnnx(self):
        cnnx = snowflake.connector.connect(
            user=self.cnnx_params["user"],
            password=self.cnnx_params["pwd"],
            account=self.cnnx_params["account"],
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
    test_api = SnowflakeAPI(db="flipside")
    print(test_api.cnnx_params)
    test_cnnx = test_api.get_cnnx()
    print(test_cnnx)
