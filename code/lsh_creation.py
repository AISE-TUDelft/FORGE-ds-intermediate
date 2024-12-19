import json
import pickle
from datasketch import MinHash, MinHashLSH
from datasets import load_dataset, disable_caching
import hashlib
import struct
from tqdm import tqdm
from huggingface_hub import login
import ast
import regex as re

disable_caching()
login("HUGGINGFACE KEY")

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
    content = re.sub(r"--.*", "", content)
    content = remove_nested("{-", "-}", content)
    return content


def remove_comments_coq(content):
    content = remove_nested("(*", "*)", content)
    return content


def remove_comments_fsharp(content):
    content = re.sub(r"\/\/.*", "", content)
    content = remove_nested("(*", "*)", content)
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
            "c++",
            "c#",
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


def sha256_hash128(data):
    """Generate a 128-bit hash using SHA-256.

    Args:
        data (bytes): The data to generate a hash from.

    Returns:
        int: A 128-bit integer hash value.
    """
    hash_value = hashlib.sha256(data).digest()[:16]
    return struct.unpack("<QQ", hash_value)[0]


def minhash_data(doc):
    """Calculates the minhash for each file content using 7-shingles

    Args:
        doc (dict): Dictionary containing the content of the file

    Returns:
        dict: Returns dictionary with the minhash value
    """
    if "code" in doc.keys():
        content_key = "code"
    elif "text" in doc.keys():
        content_key = "text"
    elif "code_text" in doc.keys():
        content_key = "code_text"
    else:
        content_key = "content"
    minhash = MinHash(num_perm=128, hashfunc=sha256_hash128)
    processed_content = remove_comments(doc[content_key], get_langs(doc))
    text = ''.join(processed_content.lower().split())
    # text = doc["content"].lower().replace(" ", "")
    shingles = set([text[i : i + 7] for i in range(len(text) - 7 + 1)])
    for shingle in shingles:
        minhash.update(shingle.encode("utf-8"))
    return {"minhash": minhash.digest()}

if __name__ == "__main__":
    """
    Creates and saves on disk the LSH object containing all the minhashes of the files in our custom dataset.
    """

    ds = load_dataset(
        "HUGGINGFACE DATASET",
        "HaskellExact",
        split="train",
        cache_dir="/huggingfaceCache",
    )

    lsh = MinHashLSH(threshold=0.7, num_perm=128, weights=(0.4, 0.6))

    ds2 = ds.map(minhash_data, num_proc=64)
    for idx, elem in tqdm(enumerate(ds2), total=len(ds2)):
        minhash = MinHash(
            num_perm=128, hashvalues=elem["minhash"], hashfunc=sha256_hash128
        )
        lsh.insert(idx, minhash, check_duplication=False)

    with open("/lshFolder/lsh_haskell.pkl", "wb") as file:
        pickle.dump(lsh, file)
