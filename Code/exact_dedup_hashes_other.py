import subprocess
import os
from tqdm import tqdm

files = os.popen("ls ../ours").read().split("\n")
files = [f for f in files if "uniq" in f]

for f in tqdm(files):
    subprocess.call(f"cat hashes.sorted.uniq ../<Other dataset name>/{f} | sort | uniq -d > duplicates_{f}", shell = True)
