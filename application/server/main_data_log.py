import scripts.DataLogger as dlogr


CG_ASSETS = ["btc", "eth", "ada", "matic", "sol"]
data_logger = dlogr.DataLogger(assets=CG_ASSETS)
data_logger.run_logger()

