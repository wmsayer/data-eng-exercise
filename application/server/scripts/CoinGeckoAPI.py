from application.server.scripts.Admin import run_rest_get
import application.server.scripts.SnowflakeAPI as snwflk
import pandas as pd
import time
import numpy as np
import pathlib
from sys import platform
from dotenv import load_dotenv
import os
import datetime as dt
import pytz

PROJECT_ROOT = "%s" % pathlib.Path(__file__).parent.parent.parent.parent.absolute()

# CG Documentation
# https://www.coingecko.com/en/api/documentation

if platform == "linux":
    load_dotenv('/home/ubuntu/.bashrc')

SNWFLK_DB = os.environ.get('SNOWFLAKE_DB')
REQUEST_ERROR_SLEEP = 65


class CoinGeckoAPI:
    def __init__(self, assets=[], dbt_env="dbt_output_dev"):
        self.api_root = 'https://api.coingecko.com/api/v3'
        self.data_key = "coins"
        self.print_summ = False
        self.snwflk_db = SNWFLK_DB
        self.dbt_env = dbt_env

        self.cg_api_mapper = self.get_api_map_snwflk()

        self.assets = assets

        self.log_book = {
            "coingecko-trending": {"fn": self.log_trending_src, "freq": 3600*6},
            # "coingecko-trending-prices": {"fn": self.log_trending_prices, "freq": 3600*12},
            "coingecko-global": {"fn": self.get_global_mkt_cap, "freq": 3600*6},
            # "coingecko-historical_prices": {"fn": self.get_asset_mkt_chart, "freq": 3600*2},
        }

    def get_api_map_snwflk(self):
        snwflk_api = snwflk.SnowflakeAPI(schema='COINGECKO', db=self.snwflk_db)
        query = f"SELECT * FROM BIGDORKSONLY.{self.dbt_env}.PROJECT_API_MAP"
        api_mapper = snwflk_api.run_get_query(query).set_index(keys="SYMBOL")
        return api_mapper

    def get_api_map_local(self):
        mapper_path = "/".join([PROJECT_ROOT, "seeds/project_api_map.csv"])
        api_mapper = pd.read_csv(mapper_path).set_index(keys="SYMBOL")
        return api_mapper

    def get_api_map_trending(self):
        snwflk_api = snwflk.SnowflakeAPI(schema='COINGECKO', db=self.snwflk_db)
        query = "SELECT * FROM BIGDORKSONLY.COINGECKO.TRENDING_ASSETS"
        assets_df = snwflk_api.run_get_query(query).set_index(keys="SYMBOL")
        return assets_df

    def log_trending_src(self, store_local=True, write_snwflk=True):
        url = "/".join([self.api_root, "search/trending"])
        result_dict, status_code = run_rest_get(url, params={}, print_summ=self.print_summ)
        while status_code != 200:
            print(f'\tStatus code: {status_code} --- Sleep {REQUEST_ERROR_SLEEP} seconds.')
            time.sleep(REQUEST_ERROR_SLEEP)  # sleep X seconds then try again
            result_dict, status_code = run_rest_get(url, params={}, print_summ=self.print_summ)

        result_df = pd.json_normalize(result_dict[self.data_key], record_prefix="")
        result_df.columns = [c.split(".")[1] for c in result_df.columns]
        keep_cols = ["id", "name", "symbol", "market_cap_rank", "score"]
        result_df = result_df[keep_cols]
        result_df.columns = [c.upper() for c in result_df.columns]

        if store_local:
            result_df_loc = result_df.copy()
            result_df_loc["_etl_timestamp"] = dt.datetime.now(pytz.utc)
            local_path = "/".join([PROJECT_ROOT, "data/cg_historical_trending_log.csv"])
            if os.path.isfile(local_path):
                print(f"\tExisting log found at: {local_path}")
                existing_df = pd.read_csv(local_path)
                write_df = pd.concat([existing_df, result_df_loc])
            else:
                print(f"\tExisting log not found at: {local_path}")
                write_df = result_df_loc

            write_df.to_csv(local_path, index=False)

        if write_snwflk:
            snwflk_api = snwflk.SnowflakeAPI(schema='COINGECKO', db=self.snwflk_db)
            snwflk_api.write_df(result_df, table='TRENDING_LOG', replace=False)

        return result_df

    def get_spot_prices(self, write_snwflk=True):

        asset_ids = list(self.cg_api_mapper.loc[self.assets, "cg_id"].values)

        url = "/".join([self.api_root, "simple/price"])
        params = {
            "ids": ",".join(asset_ids),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true"
        }

        if self.assets:
            result_dict, status_code = run_rest_get(url, params=params, print_summ=self.print_summ)
            result_df = pd.DataFrame(result_dict).transpose()
            result_df.index.rename("asset", inplace=True)
            result_df = result_df.reset_index(drop=False)

            if write_snwflk:
                result_df.columns = [c.upper() for c in result_df.columns]
                snwflk_schema = 'COINGECKO'
                snwflk_table = 'SPOT_PRICES'
                snwflk_api = snwflk.SnowflakeAPI(db=self.snwflk_db, schema=snwflk_schema)
                snwflk_api.write_df(result_df, snwflk_table, replace=True)
        else:
            result_df = pd.DataFrame()

        return result_df

    def log_trending_prices(self):
        # latest_trending_df = self.log_trending_src(write_snwflk=True)
        asset_id_map_df = self.get_api_map_trending()
        asset_tkrs = list(asset_id_map_df.index)
        self.get_asset_mkt_chart(assets=asset_tkrs, cg_id_mapper=asset_id_map_df, interval="hourly", write_snwflk=True)

    def get_asset_mkt_chart(self, assets=[], cg_id_mapper=pd.DataFrame(), days=30, base="usd",
                            interval="daily", write_snwflk=True):
        df_list = []

        # If you're pulling data from  this API and the script fails due to a 429 Error it is you are making too many
        # requests to that API. Therefore, I implement a time.sleep(X) timer to pause for X seconds every Y calls.
        # If you get a 429 Error, adjust these parameters as needed.
        count = 1
        pause_X_sec = 60
        every_Y_calls = 9

        for a in assets:

            if (count % every_Y_calls) == 0:
                print("\tFrom CoinGeckoAPI.py >>> get_asset_mkt_chart(): Pausing %d sec for API..." % pause_X_sec)
                time.sleep(pause_X_sec)

            a_id = cg_id_mapper.loc[a, "CG_ID"]
            url = "/".join([self.api_root, "coins", a_id, "market_chart"])
            params = {'vs_currency': base, 'days': days, "interval": interval}
            result_dict, status_code = run_rest_get(url, params=params, print_summ=False)
            while status_code != 200:
                print(f'\tStatus code: {status_code} --- Sleep {REQUEST_ERROR_SLEEP} seconds.')
                time.sleep(REQUEST_ERROR_SLEEP)  # sleep X seconds then try again
                result_dict, status_code = run_rest_get(url, params=params, print_summ=False)

            result_df = pd.DataFrame()
            for k, v in result_dict.items():
                temp_df = pd.DataFrame(v, columns=["time", k])

                if "time" in result_df.columns:
                    result_df[k] = temp_df[k]
                else:
                    result_df = temp_df

            result_df["cg_id"] = a_id
            result_df["symbol"] = a
            result_df.sort_values("time", inplace=True)
            df_list.append(result_df)

            count += 1

        df = pd.concat(df_list)
        df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)

        if interval == "daily":
            df['time'] = df['time'] - np.timedelta64(1, 's')  # this is to make sure prices represent close not open

        df["date"] = df['time'].dt.strftime('%Y-%m-%d')
        df["hour"] = pd.to_datetime(df['time'].dt.round("H"), utc=True).dt.hour
        df['time'] = df['time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df.drop_duplicates(subset=["cg_id", "date", "hour"], inplace=True)

        if write_snwflk:
            snwflk_api = snwflk.SnowflakeAPI(schema='COINGECKO', db=self.snwflk_db)
            snwflk_api.write_df(df, table=f'HISTORICAL_PRICES_{interval.upper()}', replace=True)

        return df

    def get_global_mkt_cap(self, write_snwflk=False, store_local=True):
        url = "/".join([self.api_root, "global"])
        result_dict, status_code = run_rest_get(url, params={}, print_summ=self.print_summ)
        while status_code != 200:
            print(f'\tStatus code: {status_code} --- Sleep {REQUEST_ERROR_SLEEP} seconds.')
            time.sleep(REQUEST_ERROR_SLEEP)  # sleep X seconds then try again
            result_dict, status_code = run_rest_get(url, params={}, print_summ=self.print_summ)

        result_dict = result_dict["data"]
        result_dict["total_market_cap"] = result_dict["total_market_cap"]["usd"]
        result_dict["total_volume"] = result_dict["total_volume"]["usd"]
        result_dict.pop("market_cap_percentage")
        result_df = pd.DataFrame([result_dict])
        result_df.columns = [c.upper() for c in result_df.columns]
        result_df["UPDATED_AT"] = pd.to_datetime(result_df["UPDATED_AT"], unit='s', utc=True)

        result_df["DATE"] = result_df['UPDATED_AT'].dt.strftime('%Y-%m-%d')
        result_df["HOUR"] = pd.to_datetime(result_df['UPDATED_AT'].dt.round("H"), utc=True).dt.hour
        result_df['UPDATED_AT'] = result_df['UPDATED_AT'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        if store_local:
            local_path = "/".join([PROJECT_ROOT, "data/cg_global_stats.csv"])
            if os.path.isfile(local_path):
                print(f"\tExisting log found at: {local_path}")
                existing_df = pd.read_csv(local_path)
                write_df = pd.concat([existing_df, result_df])
            else:
                print(f"\tExisting log not found at: {local_path}")
                write_df = result_df
            write_df.to_csv(local_path, index=False)

        if write_snwflk:
            snwflk_api = snwflk.SnowflakeAPI(schema='COINGECKO', db=self.snwflk_db)
            snwflk_api.write_df(result_df, table=f'GLOBAL_STATS', replace=False)

    def init_snwflk_trending(self):
        local_path = "/".join([PROJECT_ROOT, "data/cg_historical_trending_log.csv"])
        existing_df = pd.read_csv(local_path)
        existing_df.columns = [c.upper() for c in existing_df.columns]

        snwflk_api = snwflk.SnowflakeAPI(schema='COINGECKO', db="BIGDORKSONLY")
        snwflk_api.write_df(existing_df, table='TRENDING_LOG', replace=True)


if __name__ == "__main__":
    test_assets = ["btc", "eth", "ada", "matic", "sol"]
    test_api = CoinGeckoAPI(test_assets)

    test_api.init_snwflk_trending()
