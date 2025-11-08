import argparse
import os
import pandas as pd
from loguru import logger

# ---------------------------------------
# 1️⃣ Parse command-line arguments
# ---------------------------------------
parser = argparse.ArgumentParser(description="Temperature Average Calculator")
parser.add_argument("--config", required=True, help="Path to config file")
parser.add_argument("--env", required=True, choices=["qa", "prod"], help="Environment (qa or prod)")
parser.add_argument("--format", required=True, choices=["csv", "parquet"], help="File format (csv or parquet)")
args = parser.parse_args()

# ---------------------------------------
# 2️⃣ Read config file
# ---------------------------------------
config_data = {}
with open(args.config, "r") as file:
    for line in file:
        if "=" in line:
            key, value = line.strip().split("=", 1)
            config_data[key.strip()] = value.strip()

# Pick values based on env (qa/prod)
env_prefix = args.env.lower()

input_path = config_data.get(f"{env_prefix}_input_path")
output_path = config_data.get(f"{env_prefix}_output_path")
input_file = config_data.get(f"{env_prefix}_input_file")
output_file = config_data.get(f"{env_prefix}_output_file")

input_file_path = os.path.join(input_path, input_file)
output_file_path = os.path.join(output_path, output_file)

# ---------------------------------------
# 3️⃣ Logging setup
# ---------------------------------------
logger.add("app.log", rotation="1 MB")
logger.info(f"Environment selected: {args.env.upper()}")
logger.info(f"File format: {args.format}")
logger.info(f"Input file: {input_file_path}")

# ---------------------------------------
# 4️⃣ Read the input data
# ---------------------------------------
try:
    if args.format == "csv":
        df = pd.read_csv(input_file_path)
    else:
        df = pd.read_parquet(input_file_path)
    logger.info(f"Data read successfully. Rows: {len(df)} Columns: {len(df.columns)}")
except Exception as e:
    logger.error(f"Error reading input file: {e}")
    exit(1)


required_columns = ["Country", "State", "AvgTemperature"]


missing_cols = [col for col in required_columns if col not in df.columns]
if missing_cols:
    logger.error(f"Missing required columns: {missing_cols}")
    exit(1)


before_rows = len(df)
df = df.dropna(subset=required_columns)
after_rows = len(df)

logger.info(f"Data cleaning done. Dropped {before_rows - after_rows} rows with null values.")


try:
    grouped = df.groupby(["Country", "State"]).agg({"AvgTemperature": "mean"}).reset_index()
    grouped.rename(columns={"AvgTemperature": "Avg_Temperature"}, inplace=True)
    logger.info("Average temperature calculated successfully.")
except Exception as e:
    logger.error(f"Error during calculation: {e}")
    exit(1)


if os.path.exists(output_file_path):
    os.remove(output_file_path)
    logger.warning("Old output file deleted.")


os.makedirs(output_path, exist_ok=True)
try:
    if args.format == "csv":
        grouped.to_csv(output_file_path, index=False)
    else:
        grouped.to_parquet(output_file_path, index=False)
    logger.info(f"Output saved successfully at: {output_file_path}")
except Exception as e:
    logger.error(f"Error saving output: {e}")
    exit(1)

print("Task Completed Successfully!")
