import json
import sys
from datasets import load_dataset, load_from_disk, Dataset
import hashlib
import os
import pandas as pd
from huggingface_hub import login
import ast
import regex as re
from huggingface_hub import HfFileSystem

login("HUGGINGFACE KEY")

def dedup(row, public_hashes):
    row['exact_dupe_RedPajama'] = row['sha'] in public_hashes
    return row


def process_language(lang):
    """
    Removes exact duplicate files within our custom dataset and between our custom dataset and another public dataset.

    Args:
        our_data, pub_data (str): Names of our custom dataset and other public dataset. You should use the config name (e.g. JavaFiles, PythonFiles, etc...), since the repo will be the same.

    The new dataset without exact duplicates is pushed to the huggingface hub.

    """
    our_dataset = load_dataset(
        "REPO_NAME",
        lang+"Exact",
        split="train",
        num_proc=16
    )
    with open(f"./hashes/hashes_{lang}.sorted.uniq", "r") as infile:
        self_uniq_hashes = infile.read().split()

    with open(f"./hashes/RedPajama/duplicates_hashes_{lang}.sorted.uniq", "r") as infile:
        public_hashes = infile.read().split()

    public_hashes_set = set(public_hashes)

    our_dataset = our_dataset.map(lambda row: dedup(row, public_hashes_set), num_proc = 64)

    our_dataset.push_to_hub(
        "REPO_NAME",
        lang + "Exact",
        data_dir=f"data/{lang}_Exact",
    )


if __name__ == "__main__":
    fs = HfFileSystem()
    subset_names = [x.split('/')[-1] for x in fs.ls("REPOSITORY", detail=False)]
    subset_names = [x for x in subset_names if "Exact" in x]

    subset_names.sort()
    print(subset_names)

    for i, dsn in enumerate(subset_names):
        print(dsn)
        print(i)
        process_language(dsn)
