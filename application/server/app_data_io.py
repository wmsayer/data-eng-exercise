import pandas as pd
import scripts.SnowflakeAPI as snwflk
from scripts.Admin import format_num_to_sig_figs


class AppDataIO:
    def __init__(self, snwflk_api, dbt_env):
        self.dbt_env = dbt_env
        self.snwflk_api = snwflk_api
        self.trending_df = pd.DataFrame()
        self.trending_projects_df = pd.DataFrame()
        self.curr_trend_summ_df = pd.DataFrame()
        self.trending_asset_ids = []
        self.assets_by_trending_24hr = []

        self.refresh_data()

    def refresh_data(self):
        self.get_trending_data()
        self.get_curr_trend_summ()

    def get_btc_price_data(self):
        query = """
        SELECT *
        FROM BIGDORKSONLY.coingecko.historical_prices
        ORDER BY TIME
        """
        result_df = self.snwflk_api.run_get_query(query)
        result_df['TIME'] = pd.to_datetime(result_df['TIME'], utc=True)
        return result_df

    def get_spot_prices(self):
        table_str = "BIGDORKSONLY.cryptowatch.spot_prices"
        query = f"""
            SELECT _ETL_TIMESTAMP AS TIME, ASSET, BASE, PRICE
            FROM {table_str}
            WHERE _ETL_TIMESTAMP = (SELECT MAX(_ETL_TIMESTAMP) FROM {table_str})
            """
        result_df = self.snwflk_api.run_get_query(query)
        result_df['TIME'] = pd.to_datetime(result_df['TIME'], utc=True)
        return result_df

    def get_trending_data(self):
        query = f"""
                SELECT *
                FROM BIGDORKSONLY.{self.dbt_env}.historical_trending_hourly
            """

        result_df = self.snwflk_api.run_get_query(query)
        result_df['TIME'] = pd.to_datetime(result_df['TIME'], utc=True)

        projects_df = result_df[["CG_ID", "NAME", "SYMBOL"]].drop_duplicates().set_index(keys="CG_ID")
        projects_df.columns = [c.replace("_", " ").title() for c in list(projects_df.columns)]
        self.trending_projects_df = projects_df

        result_df.set_index(keys="CG_ID", inplace=True)
        result_df.columns = [c.replace("_", " ").title() for c in list(result_df.columns)]

        self.trending_df = result_df

    def get_available_asset_opts(self):
        opts = [{'label': self.trending_projects_df.loc[cg_id, "Name"], 'value': cg_id} for cg_id in self.trending_asset_ids]
        return opts

    def get_curr_trend_summ(self):
        query = f"""
                SELECT *
                FROM BIGDORKSONLY.{self.dbt_env}.curr_trending_summary
            """

        result_df = self.snwflk_api.run_get_query(query)
        result_df['TIME'] = pd.to_datetime(result_df['TIME'], utc=True)
        result_df['GLB_LAST_LOG_TAKEN'] = pd.to_datetime(result_df['GLB_LAST_LOG_TAKEN'], utc=True)
        result_df['ASSET_LAST_LOG_TAKEN'] = pd.to_datetime(result_df['ASSET_LAST_LOG_TAKEN'], utc=True)
        result_df['Last Seen Trending'] = result_df['ASSET_LAST_LOG_TAKEN'].dt.strftime('%Y-%m-%d %H:%M')
        # result_df.rename(columns={"ASSET_LAST_LOG_TAKEN": "Last Seen Trending"}, inplace=True)

        result_df.set_index(keys="CG_ID", inplace=True)
        result_df.columns = [c.replace("_", " ").title() for c in list(result_df.columns)]

        result_df = result_df.loc[result_df["Market Cap Rank"] < 500, :]

        for c in result_df.columns:
            if "Trending Score" in c:
                result_df[c] = 100 * result_df[c] / result_df[c].max()

        self.curr_trend_summ_df = result_df
        self.trending_asset_ids = sorted(list(set(list(self.curr_trend_summ_df.index))))
        self.assets_by_trending_24hr = list(result_df.sort_values(by="Trending Score 24Hr", ascending=False).index)


if __name__ == '__main__':
    test_dbt_env = "dbt_output_dev"
    test_api = snwflk.SnowflakeAPI(schema=test_dbt_env, wh="APPLICATION")
    test_io = AppDataIO(test_api, test_dbt_env)
    test_io.get_curr_trend_summ()
    print(test_io.curr_trend_summ_df.columns)
    print(test_io.curr_trend_summ_df)

