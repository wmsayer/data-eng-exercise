import scripts.Flipside as flip
import scripts.CoinGeckoAPI as cg
import scripts.CryptowatchAPI as cw
import logging
import time
import pathlib

PROJECT_ROOT = "%s" % pathlib.Path(__file__).parent.parent.parent.parent.absolute()


class DataLogger:
    def __init__(self, log_env="dev", dbt_env="dbt_output_dev", assets=[]):
        self.log_env = log_env
        self.assets = assets
        self.apis = [
            cg.CoinGeckoAPI(dbt_env=dbt_env, assets=self.assets),
            cw.CryptowatchAPI(assets=self.assets),
            # flip.Flipside()
        ]
        self.min_sleep = self.get_min_sleep()

        self.status_log_fn = f"data_log.txt"
        self.status_log_dir = "/".join([PROJECT_ROOT, "application/server/logs"])
        self.status_log_path = "/".join([self.status_log_dir, self.status_log_fn])
        self.setup_logger()

    def get_min_sleep(self):
        freq_list = []
        last_time = 0

        for api in self.apis:
            for log_i, log_dict in api.log_book.items():
                freq_list.append(log_dict["freq"])
                log_dict["last_run"] = last_time

        return min(freq_list)

    def setup_logger(self):
        logging.basicConfig(filename=self.status_log_path,
                            level=logging.INFO,
                            format='%(levelname)s: %(asctime)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    def log_data(self):
        run_time = time.time()
        logging.info(f"Run - {round(run_time)}")
        for api in self.apis:
            for log_i, log_dict in api.log_book.items():
                delta = (run_time - log_dict["last_run"])
                if log_dict["freq"] <= delta:
                    logging.info(f"-----Running {log_i} at freq = {log_dict['freq']} (delta = {round(delta, 1)})-------")
                    log_dict["last_run"] = run_time
                    log_dict["fn"]()
        return run_time

    def run_logger(self):
        run_time = self.log_data()
        logging.info(f"Run - {round(run_time)} complete.")
        logging.info("/" * 69)
