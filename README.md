# The Heap
A contamination free code dataset for the evaluation and investigation of LLM behavior. 

[HuggingFace](https://huggingface.co/datasets/WizzF/Heap-Forge)

## Layout
We give the code to reproduce the dataset in the code folder.

In the repositories folder, we give a list of all repositories we used to generate the dataset.
## Running the code
### Code Collection
1. We start by scraping repositories from GitHub based on their creation date, license, and amount of stars, using _repo_extract.js_.
2. We extract all files corresponding to the selected language from each repository, using _extract_files.py_.

### Exact Deduplication
To run the exact deduplication we make use of unix (ubuntu) tools, the naming/availability may differ depending on the OS.
1. First we run _hash_entries.py_ To calculate and save to a text file all hashes belonging to our and other datasets.
2. We generate lists of unique hashes of our dataset, and the other dataset using _exact_dedup_hashes_self.py_.
3. We merge two sets of hashes and record the duplicates using _exact_dedup_hashes_other.py_.
4. We flag duplicates in our dataset with respect to other datasets using _exact_dedup_dataset.py_.

### Near Deduplication
1. We generate and save the LSH object containing all the minhashes of our exact deduplicated dataset, using _lsh_creation.py_.
2. Using the LSH object, we perform near deduplication against other public datasets, using _near_dedup.py_. 

## Using the dataset
In order to have the most data available for each dataset, we do not filter duplicates from the dataset. Instead we add a boolean mask to The Heap that allows for filtering for unique files in each dataset.

Using the Datasets API, our dataset can be used as follows:

```python
from datasets import load_dataset

dataset_name = 'redpajama'
language = 'Python'

ds = load_dataset(
    "WizzF/Heap-Forge",
    f"{language}",
    split="train",
    num_proc=16
)

ds = ds.filter(lambda x: not x[f'exact_duplicates_{dataset_name}'] and not x[f'near_duplicates_{dataset_name}'])
```

## Acknowledgements
We extended the collection of programming language extensions used for [The Stack](https://gist.github.com/ppisarczyk/43962d06686722d26d176fad46879d41), in the file _langs_extension.json_
We added the EJS, Raku, Starlark, and WebAssembly languages. 
