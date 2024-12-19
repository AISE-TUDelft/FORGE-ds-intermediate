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


login("HUGGINGFACE TOKEN")


def remove_comments_java(content):
    content = re.sub(r"\/\*[\S\s]*?\*\/", "", content)
    content = re.sub(r"\\{2}.*.*", "", content)
    return content


def remove_comments_python(content):
    content = re.sub(r"#.*\\n", "", content)
    content = re.sub(r"\"{3}([\S\s]*?)\"{3}", "", content)
    return content


def remove_comments_erlang(content):
    content = re.sub(r"%.*", "", content)
    return content


def remove_comments_julia(content):
    content = re.sub(r"#=([\S\s]*?)=#", "", content)
    content = re.sub(r"#.*", "", content)
    return content


def remove_comments_lisp(content):
    content = re.sub(r";.*", "", content)
    return content


def remove_comments_fortran(content):
    content = re.sub(r"!.*", "", content)
    return content


def remove_comments_cobol(content):
    content = re.sub(r"(^|\n).{6}(\*|/).*", "\n", content)
    content = re.sub(r"\*>.*", "", content)
    return content

def remove_comments_html(content):
    content = re.sub(r"<!--([\S\s]*?)-->", "", content)
    return content


def remove_comments_matlab(content):
    content = re.sub(r"%{([\S\s]*?)%}", "", content)
    content = re.sub(r"%.*", "", content)
    return content


def remove_comments_webassembly(content):
    content = re.sub(r"\(;([\S\s]*?);\)", "", content)
    content = re.sub(r";;.*", "", content)
    return content


def remove_comments_assembly(content):
    content = re.sub(r";.*", "", content)
    return content


def remove_comments_ruby(content):
    content = re.sub(r"#.*", "", content)
    content = re.sub(r"=begin([\S\s]*?)=end", "", content)
    return content


def remove_comments_mathematica(content):
    content = remove_nested("(*", "*)", content)
    return content


def remove_comments_ada(content):
    content = re.sub(r"--.*", "", content)
    return content


def remove_comments_agda(content):
    content = remove_nested("{-", "-}", content)
    content = re.sub(r"--.*", "", content)
    return content


def remove_comments_coq(content):
    content = remove_nested("(*", "*)", content)
    return content


def remove_comments_fsharp(content):
    content = remove_nested("(*", "*)", content)
    content = re.sub(r"\/\/.*", "", content)
    return content


def remove_comments_d(content):
    content = re.sub(r"\/\*\*[\S\s]*?\*\/", "", content)
    content = re.sub(r"\/\+\+[\S\s]*?\+\/", "", content)
    content = re.sub(r"\/\/\/.*", "", content)
    return content


def remove_comments_forth(content):
    content = re.sub(r"\\\s.*", "", content)
    content = re.sub(r"\(\s[\s\S]*?\s\)", "", content)
    return content


def remove_comments_lua(content):
    content = re.sub(r"--\[\[[\s\S]*?\]\]", "", content)
    content = re.sub(r"--.*", "", content)
    return content


def remove_comments_perl(content):
    content = re.sub(r"=[\s\S]*?=cut", "", content)
    content = re.sub(r"#.*", "", content)
    return content


def remove_comments_prolog(content):
    content = re.sub(r"%.*", "", content)
    content = re.sub(r"\/\*[\s\S]*?\*\/", "", content)
    return content


def remove_comments_raku(content):
    content = re.sub(r"#'\([\s\S]*?\)", "", content)
    content = re.sub(r"#'\{[\s\S]*?\}", "", content)
    content = re.sub(r"#'\[[\s\S]*?\]", "", content)
    content = re.sub(r"#'\<[\s\S]*?\>", "", content)
    content = re.sub(r"#.*", "", content)
    return content


def remove_comments_sql(content):
    content = re.sub(r"\/\*[\s\S]*?\*\/", "", content)
    content = re.sub(r"--.*", "", content)
    return content

def remove_nested(delim_start, delim_end, content):
    content = list(content)
    merged_content = []
    skip = False
    stride = len(delim_start)
    for i in range(len(content)):
        if not skip:
            test = content[i : i + stride]
            if "".join(test) == delim_start:
                merged_content.append("".join(test))
                skip = True
                continue
            elif "".join(test) == delim_end:
                merged_content.append("".join(test))
                skip = True
                continue
            else:
                merged_content.append(test[0])
        skip = False

    return_content = ""
    save = 0
    prev = 0
    for i in range(len(merged_content)):
        test = merged_content[i]
        if test == delim_start:
            save = save + 1
        if test == delim_end:
            save = save - 1
            continue
        if save <= 0:
            return_content += test
    return return_content


def ending_to_langs(ending):
    ending = "." + ending.lower()
    langs = []
    with open("langs_extension.json", "rb") as infile:
        ending_map = infile.read()
        ending_map = json.loads(ending_map)
    for row in ending_map:
        if "extensions" in row.keys() and ending in row["extensions"]:
            langs.append(row["name"])
    return langs

def get_langs(entry):
    """
    Get the language of the code, this will change depending on the dataset.
    """
    # The Stack V2, GithubCode, codeparrot
    if "path" in entry.keys():
        path = entry["path"]
    # The Stack v1
    elif "max_stars_repo_path" in entry.keys():
        path = entry["max_stars_repo_path"]
    # Red pajama
    elif "meta" in entry.keys():
        meta_data = entry["meta"]
        meta_data = ast.literal_eval(meta_data)
        path = meta_data["path"]
    # CodeClippy
    elif "file_name" in entry.keys():
        path = entry["file_name"]
    elif "file_path" in entry.keys():
        path = entry["file_path"]
    file_ending = path.split(".")[-1]
    langs = ending_to_langs(file_ending)
    return langs

def remove_comments(content, langs):
    """
    We remove content for all applicable langs, due to naming collisions in file endings, where multiple languages use the same ending.
    This approach will introduce more false positives (duplicates that are not duplicates), however, no false negatives which we are more concerned about.
    """
    langs = [lang.lower() for lang in langs]
    for lang in langs:
        if lang in [
            "java",
            "c",
            "cpp",
            "c++",
            "c#",
            "csharp",
            "c-sharp",
            "javascript",
            "typescript",
            "objective-c",
            "go",
            "kotlin",
            "vue",
            "scala",
            "dart",
            "rust",
            "hack",
            "less",
            "groovy",
            "processing",
            "apex",
            "cuda",
            "scilab",
            "antlr",
            "swift",
            "php",
        ]:
            content = remove_comments_java(content)
        elif lang in ["python", "r", "elixir", "nix", "starlark", "graphql", "crystal"]:
            content = remove_comments_python(content)
        elif lang in ["ada"]:
            content = remove_comments_ada(content)
        elif lang in ["agda", "elm"]:
            content = remove_comments_agda(content)
        elif lang in ["assembly", "netlogo", "scheme"]:
            content = remove_comments_assembly(content)
        elif lang in ["cobol"]:
            content = remove_comments_cobol(content)
        elif lang in ["coq", "ocaml"]:
            content = remove_comments_coq(content)
        elif lang in ["d"]:
            content = remove_comments_d(content)
        elif lang in ["erlang"]:
            content = remove_comments_erlang(content)
        elif lang in ["f#"]:
            content = remove_comments_fsharp(content)
        elif lang in ["forth"]:
            content = remove_comments_forth(content)
        elif lang in ["fortran"]:
            content = remove_comments_fortran(content)
        elif lang in ["julia"]:
            content = remove_comments_julia(content)
        elif lang in ["Lisp"]:
            content = remove_comments_lisp(content)
        elif lang in ["lua"]:
            content = remove_comments_lua(content)
        elif lang in ["mathematica"]:
            content = remove_comments_mathematica(content)
        elif lang in ["matlab"]:
            content = remove_comments_matlab(content)
        elif lang in ["perl"]:
            content = remove_comments_perl(content)
        elif lang in ["prolog"]:
            content = remove_comments_prolog(content)
        elif lang in ["raku"]:
            content = remove_comments_raku(content)
        elif lang in ["ruby"]:
            content = remove_comments_ruby(content)
        elif lang in ["sql"]:
            content = remove_comments_sql(content)
        elif lang in ["webassembly"]:
            content = remove_comments_webassembly(content)
    return content

def sha256_checksum_text(entry, idx):
    """
    Computes and returns the SHA-256 checksum for the given text content which is lowercased and all whitespaces are removed.

    Args:
        content (dict): Dictionary with a "content" key containing the text.

    Returns:
        dict: Original file content with an added "sha" key for the checksum.
    """
    datakey = 'content'
    if 'text' in entry:
        datakey = 'text'
    elif 'code' in entry:
        datakey = 'code'
    elif 'code_text' in entry:
        datakey = 'code_text'
    try:
        processed_content = remove_comments(entry[datakey], get_langs(entry))
    except:
        processed_content = ""
    processed_content = "" if processed_content == None else processed_content
    processed_content = "".join(processed_content.lower().split())
    sha256 = hashlib.sha256()
    sha256.update(processed_content.encode("utf-8"))
    entry["id"] = idx
    entry["sha"] = sha256.hexdigest()
    return entry
  
if __name__ == "__main__":
    subsets = ['ANTLR', 'Ada', 'Agda', 'Apex', 'Assembly', 'CPP', 'C#', 'COBOL', 'Clojure', 'CommonLisp', 'Coq', 'Crystal', 'Cuda', 'D', 'Dart', 'EJS', 'Elixir', 'Elm', 'EmacsLisp', 'Erlang', 'F#', 'Forth', 'Fortran', 'Go', 'GraphQL', 'Groovy', 'Hack', 'Haskell', 'JavaScript', 'Java', 'Julia', 'JupyterNotebook', 'Kotlin', 'Less', 'Lua', 'Mathematica', 'Matlab', 'NetLogo', 'NewLisp', 'Nix', 'OCaml', 'Objective-C', 'PHP', 'Pascal', 'Perl', 'Processing', 'Prolog', 'Python', 'R', 'Raku', 'Ruby', 'Rust', 'SQL', 'Scala', 'Scheme', 'Scilab', 'Starlark', 'Swift', 'Turtle', 'TypeScript', 'Vue', 'WebAssembly']
    for i, subset in enumerate(subsets):
        print(subset)
        print(i)
#       In case the dataset doesn't fit in memory or on the disk
#        windows = [(0,25), (25,50), (50,75), (75,100)]
        windows = [(0,100)]
        for lower, upper in windows:
            if lower < 0:
                continue
            print(f'section {lower}%, {upper}%')
            public_dataset = load_dataset(
                "DATASET NAME",
                f"SUBSET NAME",
                split="train",
                num_proc=16
            )

            public_dataset = public_dataset.map(
                sha256_checksum_text, num_proc=64, with_indices=True
            )
            with open(f"./hashes/hashes_{DATASET_NAME}{subset}", 'a+') as outfile:
                for h in public_dataset['sha']:
                    outfile.write(str(h) + '\n')

