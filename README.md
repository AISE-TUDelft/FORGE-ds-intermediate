# The Heap
A contamination free code dataset for the evaluation and investigation of LLM behavior.

## layout
We give the code to reproduce the dataset in the code folder.
In the repositories folder, we give a list of all repositories we used to generate the dataset.
## Running the code
### Exact Dedup
To run the exact deduplication we make use of unix (ubuntu) tools, the naming/availability may differ depending on the OS.
1. First we run ... To calculate and save to a text file all hashes belonging to our and other datasets.
2. We generate unique hsahes of our dataset, and the other dataset using ...
3. We merge two sets of hashes and record the duplicates using ...
4. We flag duplicates in our dataset with respect to other datasets using ...

### Near Dedup


## Using the dataset
In order to have the most data available for each dataset, we do not filter duplicates from the dataset. Instead we add a boolean mask to The Heap that allows for filtering for unique files in each dataset.

Using the Datasets API, our dataset can be used as follows:

```
from datasets import load_dataset

dataset_name = 'RedPajama'
language = 'Java'

ds = load_dataset(
    "/The_Heap",
    f"{language}Near",
    split="train",
    num_proc=16
)

ds = ds.filter(lambda x: not x[f'exact_dupe_{dataset_name}'] and not x[f'near_dups_{dataset_name}'])
```
