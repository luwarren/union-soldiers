import os
import pickle
import numpy as np

from tqdm import tqdm

def get_regiment_ids() -> np.ndarray:
    # if data/regiment_ids.pickle already exists, read it
    if os.path.exists("data/regiment_ids.pkl"):
        with open("data/regiment_ids.pkl", "rb") as f:
            return np.array(list(pickle.load(f)))

    filenames = os.listdir("data/soldier_records")

    regiment_ids = set()

    for filename in tqdm(filenames, desc = "Getting regiment ids from soldier records"):
        # open the pickle file
        with open("data/soldier_records/" + filename, "rb") as f:
            # read the file
            data = pickle.load(f)

            # extract regiment id from json object
            relations = data["relations"]
            for relation in relations:
                if relation["id"]["contentType"] == "REGIMENT":
                    regiment_ids.add(relation["id"]["objectId"])

    # save to pickle file in data
    with open("data/regiment_ids.pkl", "wb") as f:
        pickle.dump(regiment_ids, f)

    return np.array(list(regiment_ids))
