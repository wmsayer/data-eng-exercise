import pandas as pd
from snowflake.snowpark.functions import sql_expr


def model(dbt, session):
    # DataFrame representing an upstream model
    # asset_map_df = dbt.ref("cg_trending_asset_map").to_pandas().set_index(keys="ID")
    
    # DataFrame representing an upstream source
    # upstream_source_df = dbt.source("upstream_source_name", "table_name")
    raw_trending_df = dbt.source("coingecko", "trending_log")
    # asset_map_df = dbt.source("coingecko", "trending_assets")

    # asset_map_df = asset_map_df[["ID", "NAME", "SYMBOL"]]
    
    keep_cols = ["ID", "_ETL_TIMESTAMP", "MARKET_CAP_RANK", "SCORE"]
    wkg_df = raw_trending_df[keep_cols].to_pandas().sort_values(by=["ID", "_ETL_TIMESTAMP"], ascending=True)
    wkg_df['TIME'] = pd.to_datetime(wkg_df['_ETL_TIMESTAMP'], utc=True)
    wkg_df['TIME_ts'] = pd.to_datetime(wkg_df['_ETL_TIMESTAMP']).astype(int)/ 10**9
    wkg_df['DATE'] = wkg_df['TIME'].dt.date
    wkg_df['HOUR'] = wkg_df['TIME'].dt.hour

    # calc delta_ts grouped by ID
    wkg_df["delta_ts"] = wkg_df.groupby(['ID'])['TIME_ts'].rolling(window=2).apply(lambda x: x.iloc[1] - x.iloc[0]).values

    # calc new score via (16 - SCORE) / 4
    wkg_df["TRENDING_SCORE"] = wkg_df["delta_ts"] * (16 - wkg_df["SCORE"]) / 4

    # filter delta_ts > 15 minutes
    wkg_df = wkg_df.loc[wkg_df["delta_ts"] < 60*15, :]

    # normalize score
    wkg_df["TRENDING_SCORE"] = 100 * wkg_df["TRENDING_SCORE"] / wkg_df["TRENDING_SCORE"].max()
    
    # finalize output
    final_cols = ["ID", "TIME", "DATE", "HOUR", "MARKET_CAP_RANK", "TRENDING_SCORE"]
    wkg_df = wkg_df[final_cols].rename(columns={"ID": "CG_ID"})
    # wkg_df = wkg_df.join(asset_map_df, how="left", on="ID")
    final_df = session.create_dataframe(wkg_df).toDF(list(wkg_df.columns))
    final_df = final_df.withColumn("TIME", sql_expr("to_timestamp(TIME::string)"))

    return final_df