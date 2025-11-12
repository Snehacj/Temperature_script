import argparse
import os
import pandas as pd
from loguru import logger
import configparser
import sys


parser = argparse.ArgumentParser(description="Temperature Average Calculator")
parser.add_argument("--config", required=True, help="Path to config INI file")
parser.add_argument("--env", required=True, choices=["qa", "prod"], help="Environment (qa or prod)")
parser.add_argument("--format", required=True, choices=["csv", "parquet"], help="File format (csv or parquet)")
args = parser.parse_args()

logger.add("app.log", rotation="1 MB")

config = configparser.ConfigParser()

try:
    config.read(args.config)
    input_path = config[args.env]["input_path"]
    output_path = config[args.env]["output_path"]
    input_file = config[args.env]["input_file"]
    output_file = config[args.env]["output_file"]
except Exception as e:
    logger.error(f"Error reading config file: {e}")
    sys.exit(1)

input_file_path = os.path.join(input_path, input_file)
output_file_path = os.path.join(output_path, output_file)

logger.info(f"Environment: {args.env.upper()}")
logger.info(f"File format: {args.format}")
logger.info(f"Input file path: {input_file_path}")

try:
    if args.format == "csv":
        df = pd.read_csv(input_file_path)
    else:
        df = pd.read_parquet(input_file_path)
    logger.info("Data read successfully.")
except FileNotFoundError:
    logger.error(f"Input file not found: {input_file_path}")
    sys.exit(1)
except Exception as e:
    logger.error(f"Error reading input file: {e}")
    sys.exit(1)


try:
    grouped = df.groupby(["Country", "State"])["AvgTemperature"].mean().reset_index()
    logger.info("Average temperature calculated successfully.")
except KeyError as e:
    logger.error(f"Missing expected column: {e}")
    sys.exit(1)
except Exception as e:
    logger.error(f"Error during calculation: {e}")
    sys.exit(1)


try:
    if os.path.exists(output_file_path):
        os.remove(output_file_path)
        logger.warning("Old output file deleted.")

    os.makedirs(output_path, exist_ok=True)

    if args.format == "csv":
        grouped.to_csv(output_file_path, index=False)
    else:
        grouped.to_parquet(output_file_path, index=False)
    logger.info(f"Output saved successfully at: {output_file_path}")
except Exception as e:
    logger.error(f"Error saving output: {e}")
    sys.exit(1)

print("Task Completed Successfully!")
