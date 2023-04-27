import os
import pickle
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tqdm import tqdm
from collections import defaultdict

CHUNK_SIZE = 250000

def process_soldier_file(filename: str) -> pd.DataFrame:
    with open(f"data/soldier_records/{filename}", "rb") as f:
        data = pickle.load(f)
    
    facts = defaultdict(lambda : defaultdict(str)) 
    prev_military_service_type = "" # to detect when event type changes
    counter = defaultdict(int) # to append event types
    name = ""
    
    for datum in data["content"]["elements"]:
        element = datum["element"]
        # if not an event, not relevant to us
        if "event" not in element.keys():
            continue
        event_type = element["event"][0]["type"]

        # if custom event, make event type the name of the event
        if event_type == "CUSTOM":
            event_type = element["event"][0]["name"].upper()

        # if military service, make event type the name of the military service
        elif event_type == "MILITARY_SERVICE": 
            # if military service is general, make event type military service general
            if len(element["event"]) == 1:
                event_type = "MILITARY_SERVICE_GENERAL"
            else:
                event_type = "MILITARY_SERVICE_" + element["event"][1]["type"]
            
            # if military service is custom, make event type military service custom military
            if event_type == "MILITARY_SERVICE_CUSTOM_MILITARY":
                event_type = "MILITARY_SERVICE_" + element["event"][1]["name"].upper()

            # if event_type is not the same, increment counter
            if event_type != prev_military_service_type:
                counter[prev_military_service_type] += 1
                prev_military_service_type = event_type
            
            # if counter is greater than 0, append counter to event type
            if counter[event_type] > 0 and event_type != "MILITARY_SERVICE_GENERAL":
                event_type = event_type + "_" + str(counter[event_type])

        # if internal people, this is just the name
        elif event_type == "INTERNAL_PEOPLE":
            name = element["value"]
            continue

        event_type = event_type.replace(" ", "_")

        # only append to string when there is already a value
        if not facts[event_type][element["name"]]:
            facts[event_type][element["name"]] = str(element["value"])
        else:
            facts[event_type][element["name"]] += ", " + str(element["value"])
    df = pd.DataFrame.from_dict(facts, orient="index")

    df.reset_index(inplace=True)
    df.rename(columns={"index": "event_type"}, inplace=True)
    df["id"] = filename.split(".")[0] 
    df["name"] = name 

    return df

# open and unpickle a chunk of files in "data/soldier_records" directory and write to a separate CSV file
def process_chunk(chunk_filenames: list, chunk_num: int) -> str:
    with ProcessPoolExecutor() as executor:
        dfs = list(tqdm(executor.map(process_soldier_file, chunk_filenames), 
            total=len(chunk_filenames),
            desc=f"Processing chunk {chunk_num}",
            position=chunk_num))
    chunk_df = pd.concat(dfs)
    
    with ThreadPoolExecutor() as executor:
        future = executor.submit(chunk_df.to_csv, f"data/csv_out/soldier_records_chunk{chunk_num}.csv", index=False)
        future.result()
        
    return f"data/csv_out/soldier_records_chunk{chunk_num}.csv"

def compile_soldier_csv():
    filenames = os.listdir("data/soldier_records")
    chunked_filenames = [filenames[i : i + CHUNK_SIZE] for i in range(0, len(filenames), CHUNK_SIZE)]
    
    # process each chunk in parallel and collect the CSV file paths
    with ProcessPoolExecutor() as executor:
        csv_files = list(tqdm(
            executor.map(process_chunk, chunked_filenames, range(len(chunked_filenames))), 
            total=len(chunked_filenames), 
            desc="Compiling chunks"
        ))
    
    # combine all CSV files into one
    print("Compiling final CSV")
    df = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files])
    df.to_csv("data/csv_out/soldier_records.csv", index=False)

    df.reset_index(inplace=True, drop=True)
    print(df)