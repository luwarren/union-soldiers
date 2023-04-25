import os
import numpy as np
from typing import Callable

def retry_requests(
        records_dir : str,
        failed_ids_path : str,
        ids : np.ndarray,
        get_and_save_records : Callable[[np.ndarray], None],
    ):
    while True:
        filenames = os.listdir(records_dir)
        if filenames:
            filename_ids = np.array([int(filename.split(".")[0]) for filename in filenames])

            result = ids[~np.isin(ids, filename_ids)]
            print("Failed results: " + str(result.shape[0]))

            if result.shape[0] == 0:
                break

            print("Re-scraping failed results")
            ids = result

        open(failed_ids_path, "w").close()

        # run actual scraping operation
        get_and_save_records(ids)
        