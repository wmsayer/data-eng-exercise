import scripts.Flipside as flip
import scripts.CoinGeckoAPI as cg
import scripts.CryptowatchAPI as cw
import time


class DataLogger:
    def __init__(self, dbt_env="dbt_output_dev", assets=[]):
        self.assets = assets
        self.apis = [
            cg.CoinGeckoAPI(dbt_env=dbt_env, assets=self.assets),
            # cw.CryptowatchAPI(assets=self.assets),
            # flip.Flipside()
        ]
        self.min_sleep = self.get_min_sleep()

    def get_min_sleep(self):
        freq_list = []
        last_time = 0

        for api in self.apis:
            for log_i, log_dict in api.log_book.items():
                freq_list.append(log_dict["freq"])
                log_dict["last_run"] = last_time

        return min(freq_list)

    def run_logger(self):

        while True:
            curr_time = time.time()
            print("/"*69 + f"\nRun - {round(curr_time)}")
            for api in self.apis:
                for log_i, log_dict in api.log_book.items():
                    delta = (curr_time - log_dict["last_run"])
                    if log_dict["freq"] <= delta:
                        print(f"-----Running {log_i} at freq = {log_dict['freq']} (delta = {round(delta, 1)})-------")
                        log_dict["last_run"] = curr_time
                        log_dict["fn"]()
            print(f"Run - {round(curr_time)} complete. Sleep {self.min_sleep} seconds.\n" + "/"*69)
            time.sleep(self.min_sleep)  # sleep time in seconds