import scripts.DataLogger as dlogr


CG_ASSETS = ["bitcoin", "ethereum", "cardano", "polygon", "solana"]
data_logger = dlogr.DataLogger(cg_assets=CG_ASSETS)
data_logger.run_logger()

