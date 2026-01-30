# External Sorting Algorithm 

This project implements an external sorting algorithm designed to sort large binary datasets that do not fit into system memory.
The implementation simulates page-based I/O operations and buffer management to perform a multi-pass merge sort.

## 📌 Overview

The objective is to sort a massive binary file containing fixed-size records (32 bytes per record) based on a 4-byte `product_id`. 
The system must handle memory constraints defined by a specific number of buffer pages and page sizes.

### Key Features
* **External Merge Sort:** Handles datasets larger than RAM by splitting them into runs and merging them.
* **Configurable I/O:** Works with user-defined buffer page counts and page sizes.
* **Sort Order:** Supports both ascending and descending sorting.
* **Duplicate Handling:** Optional removal of duplicate records based on `product_id`.
* **Persistence:** Preserves all intermediate run and merge files for verification.

## 📂 Data Structure

* **Record Size:** 32 bytes.
* **Sort Key:** `product_id` (First 4 bytes).
* **Page Constraints:** Records are strictly stored within single pages and cannot split across page boundaries.

## 🛠️ Implementation Details

The solution is divided into three main components:

### 1. Run Generation
**Function:** `generate_runs`
* Reads the input binary file in chunks calculated by `buffer_pages * page_size`.
* Sorts each chunk in memory based on the `product_id`.
* Writes sorted chunks to disk as `run_0.bin`, `run_1.bin`, etc.
* If unique mode is enabled, duplicates are removed during this stage.

### 2. K-Way Merge
**Function:** `merge_runs`
* Combines multiple sorted run files into a single output file.
* Utilizes a $(B-1)$-way merge strategy where $B$ is the number of buffer pages (1 page for output, $B-1$ for input).
* Maintains the specified sort order (ascending/descending).

### 3. Complete External Sort
**Function:** `external_sort`
* Orchestrates the full sorting pipeline.
* **Pass 0:** Generates initial runs using `generate_runs`.
* **Subsequent Passes:** Performs multi-pass k-way merges until a single sorted file remains.
* **Intermediate Files:** Naming convention `out_pass_X_run_Y.bin` is used for merge outputs.

## 🚀 Usage

The main entry point is the `external_sort` function:

```python
def external_sort(input_filename, output_filename, buffer_pages, 
                  page_size, record_size, ascending=True, unique=False):
