import scripts.DataLogger as dlogr
from sys import platform


if platform == "linux":
    DBT_ENV = "dbt_output_prod"
else:
    DBT_ENV = "dbt_output_dev"

CG_ASSETS = ["btc", "eth", "ada", "matic", "sol"]
data_logger = dlogr.DataLogger(dbt_env=DBT_ENV, assets=CG_ASSETS)
data_logger.run_logger()

