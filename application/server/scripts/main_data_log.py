import application.server.scripts.Flipside as flip
import application.server.scripts.CoinGeckoAPI as cg
import time


CG_ASSETS = ["bitcoin", "ethereum", "cardano", "polygon", "solana"]
APIs = [cg.CoinGeckoAPI(assets=CG_ASSETS), flip.Flipside(use_sdk=True)]

freq_list = []
last_time = 0

for api in APIs:
    for log_i, log_dict in api.log_book.items():
        freq_list.append(log_dict["freq"])
        log_dict["last_run"] = last_time

min_sleep = min(freq_list)

while True:
    curr_time = time.time()
    print("/"*69 + f"\nRun - {curr_time}")
    for api in APIs:
        for log_i, log_dict in api.log_book.items():
            delta = (curr_time - log_dict["last_run"])
            if log_dict["freq"] <= delta:
                print(f"-----Running {log_i} at freq = {log_dict['freq']} (delta = {delta})-------")
                log_dict["last_run"] = curr_time
                log_dict["fn"]()

    time.sleep(min_sleep)  # sleep time in seconds

