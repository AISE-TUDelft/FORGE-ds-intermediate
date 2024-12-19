import subprocess
import os
from tqdm import tqdm

files = os.popen("ls").read().split("\n")
files = [f for f in files if "hash" in f]
for f in tqdm(files):
    subprocess.call(f"sort {f} > {f}.sorted", shell = True)
    subprocess.call(f"uniq {f}.sorted > {f}.sorted.uniq", shell = True)
