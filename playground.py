from mark_storage import ChunkWriter, cmd_read
from data_generator import generate_packet
import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Create a writer
writer = ChunkWriter("./playground_output")

# Feed it some packets
for i in range(5):
    packet = generate_packet()
    writer.add_packet(packet)
    print(f"Packet {i+1}: {len(packet['timestamps'])} particles, "
          f"buffer total: {writer._particle_counter} particles")

# Flush remaining data
writer.flush()

# Check what was written
output_dir = Path("./playground_output")

print("\nFiles written:")
for f in sorted(output_dir.iterdir()):
    print(f"  {f.name}")

print("\nIndex contents:")
print(pd.read_csv(output_dir / "index.csv"))

print("\nChunk file contents:")
parquet_file = next(output_dir.glob("*.parquet"))
df = pd.read_parquet(parquet_file)
print(f"Shape: {df.shape}")
print(df[["ts_0", "scat_0", "spec_0"]].head(3))


index_df = pd.read_csv(output_dir / "index.csv")
print(index_df)
min_ts = index_df["min_ts"].min()
max_ts = index_df["max_ts"].max()

storage_dir = "./playground_output"
start = datetime.fromtimestamp(min_ts, tz=timezone.utc).isoformat()
stop  = datetime.fromtimestamp(max_ts, tz=timezone.utc).isoformat()
args = argparse.Namespace(storage_dir=storage_dir, start=start, stop=stop)

result_df = cmd_read(args)
print(f"\nResult shape: {result_df.shape}")
print(result_df[["ts_0", "scat_0", "spec_0"]].head(3))