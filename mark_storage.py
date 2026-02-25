#!/usr/bin/env python3
"""
Basic storage implementation for the storage challenge.

Reads length-prefixed pickled packets from stdin and appends them
(re-pickled) to a single output file.

Usage:
    python data_generator.py | python basic_storage.py output.pkl
"""
import argparse
import pickle
import struct
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pathlib import Path

CHUNK_PARTICLES = 200000

def read_exact(stream, n: int) -> bytes:
    """Read exactly *n* bytes from *stream*, or return empty on EOF."""
    data = b""
    while len(data) < n:
        chunk = stream.read(n - len(data))
        if not chunk:
            return b""
        data += chunk
    return data


def get_packet_from_stream() -> [dict[str, np.array], int]:
    """Read one packet from data_generator, or return empty on EOF.

    Return a tuple (decoded data , raw data length)
    """
    stdin = sys.stdin.buffer

    # Read the 4-byte length prefix
    header = read_exact(stdin, 4)
    if not header:
        return b"", 0

    (length,) = struct.unpack(">I", header)

    # Read the payload
    payload = read_exact(stdin, length)
    if not payload:
        raise ValueError("Empty payload")

    # Deserialize
    return pickle.loads(payload), len(payload) + 4


# def write_to_storage(data: bytes, output_path: str):
#     """Add data to storage.

#     This is the naive implementation using pickle, you must do better than this!
#     """
#     with open(output_path, "ab") as fout:
#         pickle.dump(data, fout, protocol=pickle.HIGHEST_PROTOCOL)
#         fout.flush()


def cmd_write(args):
    """Write loop"""

    output_path = args.storage_dir
    total_bytes_received = 0
    packets_written = 0

    chunk_data = ChunkWriter(output_path)

    while True:
        packet, raw_data_length = get_packet_from_stream()
        if packet == b"":
            break
        chunk_data.add_packet(packet)
        packets_written += 1
        total_bytes_received += raw_data_length

    chunk_data.flush()

    print(
        f"Wrote to storage {packets_written} packets "
        f"({total_bytes_received} bytes).",
        file=sys.stderr,
    )


def cmd_read(args):
    """Read back data from storage"""
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    stop = datetime.fromisoformat(args.stop).replace(tzinfo=timezone.utc)
    start_ts = start.timestamp()
    stop_ts = stop.timestamp()
    print(f"Reading data between {start} and {stop}")
    t00 = time.monotonic()

    index_df = pd.read_csv(Path(args.storage_dir) / "index.csv")

    mask = (index_df["max_ts"] >= start_ts) & (index_df["min_ts"] <= stop_ts)
    #candidates = index_df[mask]
    results = []

    for row in index_df[mask].itertuples():
        chunk_df = pd.read_parquet(Path(args.storage_dir) / row.filename)

        time_mask = (chunk_df["ts_0"] >= start_ts) & (chunk_df["ts_0"] <= stop_ts)
        results.append(chunk_df[time_mask])

    #check if didn't found anything in that timestamp
    if not results:
        print("Found nothing in that timestamp")
        return

    df = pd.concat(results)

    print(f"Found {df.shape[0]} particles.")
    print(
        f"Read bandwidth {df.shape[0]/1024/(time.monotonic() - t00):.2f} kParticles/s."
    )

    return df


class ChunkWriter:
    def __init__(self, output_path: str):
        self._buffer = dict(timestamps = [], scattering = [], spectral = [])
        self._particle_counter = 0
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.index_path = self.output_path / "index.csv"

    def add_packet(self, packet: dict):
        self._buffer["timestamps"].append(packet["timestamps"])
        self._buffer["scattering"].append(packet["scattering"])
        self._buffer["spectral"].append(packet["spectral"])

        self._particle_counter += packet["scattering"].shape[0]

        if self._particle_counter >= 200000:
            self.flush()

    def flush(self):
        if self._particle_counter == 0:
            return
        
        timestamp_buffer = np.concatenate(self._buffer["timestamps"])
        scat_buffer = np.concatenate(self._buffer["scattering"])
        spec_buffer = np.concatenate(self._buffer["spectral"])

        scat_buffer = scat_buffer.reshape(scat_buffer.shape[0], -1)
        spec_buffer = spec_buffer.reshape(spec_buffer.shape[0], -1)

        df_ts = pd.DataFrame(data = timestamp_buffer)
        df_scat = pd.DataFrame(data = scat_buffer)
        df_spec = pd.DataFrame(data = spec_buffer)

        df_ts.columns = ['ts_0']
        df_scat.columns = [f"scat_{i}" for i in range(df_scat.shape[1])]
        df_spec.columns = [f"spec_{i}" for i in range(df_spec.shape[1])]


        df = pd.concat([df_ts, df_scat, df_spec], axis = 1)

        filename = str(timestamp_buffer[0])  + ".parquet"
        print(f"DEBUG filename: {filename}")

        df.to_parquet(self.output_path / filename)

        csv_df = pd.DataFrame(data = {"filename": [filename], "min_ts":[timestamp_buffer.min()], "max_ts":[timestamp_buffer.max()]})
        csv_df.to_csv(self.index_path, mode = "a", header = not self.index_path.exists(), index=False)

        self._particle_counter = 0
        self._buffer = dict(timestamps = [], scattering = [], spectral = [])



def main():

    parser = argparse.ArgumentParser(
        description="Example of data storage system",
    )
    parser.add_argument(
        "--storage-dir",
        type=str,
        default=None,
        help="Pickle storage file location",
    )

    sub = parser.add_subparsers(dest="command")

    # -- write --
    p_w = sub.add_parser("write", help="Ingest packets from stdin")

    # -- read --
    p_r = sub.add_parser("read", help="Query by time range")
    p_r.add_argument(
        "--start",
        required=True,
        help="Start timestamp (ISO 8601 format, inclusive)",
    )
    p_r.add_argument(
        "--stop",
        required=True,
        help="Stop timestamp (ISO 8601 format, inclusive)",
    )

    args = parser.parse_args()

    if args.command == "write":
        cmd_write(args)
    elif args.command == "read":
        cmd_read(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
