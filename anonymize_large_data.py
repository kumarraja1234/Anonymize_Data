from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col
import argparse
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def anonymize_data(input_path, output_path, columns_to_anonymize, partitions, output_format):
    try:
        logger.info("Starting Spark session...")
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("AnonymizeLargeCSV") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .getOrCreate()

        logger.info(f"Reading input file: {input_path}")
        # Load the input CSV into a DataFrame
        df = spark.read.csv(input_path, header=True)

        logger.info(f"Loaded data with {df.count()} records.")
        logger.info("Sample input data:")
        df.show(5)

        # Repartition data for distributed processing
        if partitions:
            logger.info(f"Repartitioning data into {partitions} partitions...")
            df = df.repartition(partitions)

        # Anonymize specified columns
        for column in columns_to_anonymize:
            if column in df.columns:
                logger.info(f"Anonymizing column: {column}")
                df = df.withColumn(column, sha2(col(column), 256))
            else:
                logger.warning(f"Column '{column}' not found in input data.")

        # Perform checkpointing for fault tolerance
        checkpoint_dir = "/data/checkpoints"
        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir)
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        logger.info("Checkpointing the DataFrame.")
        df = df.checkpoint()

        # Write output to specified format (Parquet for large data, CSV for simplicity)
        logger.info(f"Writing output to: {output_path} in format: {output_format}")
        if output_format == "parquet":
            df.write.parquet(output_path, mode='overwrite')
        else:
            df.write.csv(output_path, header=True, mode='overwrite')

        logger.info("Anonymization process completed successfully.")

    except Exception as e:
        logger.error(f"Error during anonymization: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anonymize Large CSV with Spark")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Path to output directory")
    parser.add_argument("--columns", nargs="+", required=True, help="Columns to anonymize")
    parser.add_argument("--partitions", type=int, default=32, help="Number of partitions")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "csv"], help="Output file format")
    args = parser.parse_args()

    anonymize_data(args.input, args.output, args.columns, args.partitions, args.output_format)
