# Plair Data Storage Challenge

> This README was generated with the assistance of an LLM (Claude by Anthropic).

## Overview

This module replaces the naive pickle-based `basic_storage.py` with a
production-quality storage system capable of handling high-throughput particle
data from Plair's calibration instruments.

## Design

### Chunked storage

Data is accumulated in memory and written to disk in chunks. This avoids
opening and closing a file for every packet, which would be extremely slow at
50,000 particles per second.

The chunking idea was inspired by a previous signal processing project
(Boulandet et al., 2021) where audio recordings were partitioned into chunks
of 128 samples before processing. The same principle applies here: instead of
processing every particle individually, we batch them together and write them
all at once when the buffer is full.

Each chunk is written as a separate Parquet file named after its first
timestamp, so files sort chronologically in the directory.

### Storage format: Parquet

Parquet was chosen after researching how to store numerical data efficiently
in Python. It is a binary columnar format that:

- stores numpy arrays compactly without text conversion overhead
- is natively supported by pandas via `to_parquet` / `read_parquet`
- requires no extra configuration (unlike HDF5 which needs PyTables)
- compresses data automatically

Pandas was used as the bridge between numpy arrays and Parquet because it was
covered extensively during a Python course as the standard tool for handling
large numerical datasets.

The 2D sensor arrays (`scattering` shape `(N, 64, 16)` and `spectral` shape
`(N, 32, 16)`) are flattened to `(N, 1024)` and `(N, 512)` respectively
before storage, since pandas DataFrames are 2D structures. Column names are
`scat_0 ... scat_1023` and `spec_0 ... spec_511`.

### Index

A lightweight `index.csv` file lives alongside the chunk files. Every time a
chunk is flushed, one row is appended recording the filename and the min/max
timestamps of that chunk.

On read, the index is consulted first to identify which files overlap the
query window — all other files are skipped entirely. This makes time-range
queries efficient regardless of how many total files exist on disk.

```
./data/
    index.csv                      ← one row per chunk
    1771582694.281498.parquet      ← chunk file
    1771582724.918273.parquet      ← next chunk
    ...
```

## Requirements

```bash
pip install numpy pandas pyarrow
```

## Usage

### Write

```bash
python data_generator.py --pps 100 --max-mb 200 \
    | python mark_storage.py --storage-dir ./data write
```

### Read

First check the index to find the timestamp range of your stored data:

```bash
cat ./data/index.csv
```

Then query using ISO 8601 timestamps that fall within that range:

```bash
python mark_storage.py --storage-dir ./data read \
    --start "2026-02-25T10:36:43" \
    --stop  "2026-02-25T10:36:44"
```

## Reference

Boulandet, R., Kham, S. R., Marmaroli, P., & Minier, J. (2021).
*Implementation and performance assessment of a MEMS-based Sound Level Meter.*
Euronoise 2021, Madeira, Portugal.