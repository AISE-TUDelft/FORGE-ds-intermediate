import os
import json
import re
import sys
import traceback
import shutil
import git
from huggingface_hub import login
from datasets import Dataset
import datasets
import subprocess
import chardet

login("HUGGINGFACE KEY")

def extract_repo_files(language, n_proj_min, n_proj_max):
    n_proj_min = int(n_proj_min)
    n_proj_max = int(n_proj_max)

    counter = 0
    language_endings = []

    with open("langs_extension.json", "r") as extensions:
        content = json.load(extensions)

    for lang in content:
        if lang["name"] == language:
            language_endings = lang["extensions"]
    #language_endings=[".star", ".bzl"]
    f = open(f"PerlCopyLeft.json", "r")
    string = f.read()
    js = json.loads(string)
    folder = "/MsrFiles/" + language + "_unseen"
    if not os.path.exists(folder):
        os.mkdir(folder)
    n_proj = n_proj_min
    for k, j in enumerate(js):
        try:
            name = j["full_name"]
            if not os.path.exists(folder + "/" + name):
                name = name.replace(".", "_")
                name = name.replace("/", "_")
                os.mkdir(folder + "/" + name)
            if j["html_url"] != None:
                subprocess.run(["git" , "clone", j["html_url"], f"{folder}/{name}"], timeout=30)
            else:
                continue
            files = [x for x in os.walk(folder + "/" + name)]
            for language_ending in language_endings:
                for f in files:
                    for f1 in f[2]:
                        basepath = f[0]
                        x = re.search(".*" + language_ending, f1)
                        if x and os.path.getsize(basepath + "/" + f1) < 10000000:
                            data = {}
                            with open(basepath + "/" + f1, "rb") as lang_file:
                                try:
                                    content = lang_file.read()
                                    encoding = chardet.detect(content)['encoding'] or 'utf-8'
                                    file_content = content.decode(encoding, errors="ignore")
                                    #file_content = re.sub(r'[\ud800-\udfff]', '', file_content)
                                    if len(file_content.split()) >= 10:
                                        data["id"] = counter
                                        data["file_name"] = f1
                                        data["file_path"] = os.path.relpath(
                                            basepath + "/" + f1, folder
                                        ).replace("\\", "/")
                                        data["content"] = file_content
                                        data["size"] = os.path.getsize(basepath + "/" + f1)
                                        data["language"] = j["language"]
                                        data["extension"] = language_ending
                                        lines = file_content.split("\n")
                                        total_line_length = 0
                                        line_count = 0
                                        max_line_length = 0
                                        total_char_count = 0
                                        alphanumeric_char_count = 0
                                        for line in lines:
                                            stripped_line = (
                                                line.strip()
                                            )  # don t count empty lines
                                            if stripped_line:
                                                total_line_length += len(stripped_line)
                                                line_count += 1
                                                total_char_count += len(line)
                                                alphanumeric_char_count += sum(
                                                    char.isalnum() for char in line
                                                )
                                                if len(line) > max_line_length:
                                                    max_line_length = len(line)
                                        data["total_lines"] = line_count
                                        if line_count > 0:
                                            data["avg_line_length"] = (
                                                float(total_line_length) / line_count
                                            )
                                        data["max_line_length"] = max_line_length
                                        if total_char_count > 0:
                                            data["alphanum_fraction"] = float(
                                                alphanumeric_char_count / total_char_count
                                            )
                                        data["repo_name"] = j["full_name"]
                                        data["repo_stars"] = j["stargazers_count"]
                                        data["repo_forks"] = j["forks_count"]
                                        data["repo_open_issues"] = j["open_issues_count"]
                                        data["repo_license"] = j["license"]["spdx_id"]
                                        data["repo_extraction_date"] = j["retrieval_date"]
                                        counter = counter + 1
                                        yield data
                                    else:
                                        continue
                                except (UnicodeEncodeError, UnicodeDecodeError):
                                    continue
                                except Exception as _:
                                    continue
                        else:
                            continue
            shutil.rmtree(folder + "/" + name)
            n_proj += 1
            if n_proj > n_proj_max:
                break
        except Exception as e:
            print(f"An exception occurred: {e}")
            traceback.print_exc()
            continue


if __name__ == "__main__":
    language = sys.argv[1] # Language name, make sure it matches the repo json file
    min_repos = sys.argv[2] # This should always be 0 when given as input
    max_repos = sys.argv[3] # This should be the total no. of repos scraped, e.g. 51000 for Java
    dataset = Dataset.from_generator(lambda: extract_repo_files(language, min_repos, max_repos))
    dataset.push_to_hub("HUGGINGFACE DATASET", config_name="PerlFiles", data_dir="data/Perl_Files")
