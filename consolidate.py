import pandas as pd
from pathlib import Path


INPUT_FOLDER = Path("out_per_file")
OUTPUT_FILE = Path("merged_output.csv")


def merge_csv_folder(input_folder: Path, output_file: Path):
    csv_files = list(input_folder.glob("*.csv"))

    if not csv_files:
        raise ValueError(f"No CSV files found in {input_folder}")

    df_list = []

    for csv_file in csv_files:
        print(f"Reading {csv_file.name}")
        df = pd.read_csv(csv_file)
        df["source_file"] = csv_file.name
        df_list.append(df)

    merged_df = pd.concat(df_list, ignore_index=True)
    merged_df.to_csv(output_file, index=False)

    print(f"\nCSV consolidated created: {output_file}")
    print(f"Total rows: {len(merged_df)}")

if __name__ == "__main__":
    merge_csv_folder(INPUT_FOLDER, OUTPUT_FILE)
