import scripts.DataLogger as dlogr
from sys import platform


if platform == "linux":
    LOG_ENV = "prod"
    DBT_ENV = "dbt_output_prod"
else:
    LOG_ENV = "dev"
    DBT_ENV = "dbt_output_dev"

CG_ASSETS = ["btc", "eth", "ada", "matic", "sol"]
data_logger = dlogr.DataLogger(log_env=LOG_ENV, dbt_env=DBT_ENV, assets=CG_ASSETS)
data_logger.run_logger()

