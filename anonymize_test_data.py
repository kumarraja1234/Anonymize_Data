from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col
import sys

def anonymize_data(input_path, output_path, columns_to_anonymize):
    try:
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("AnonymizeCSV") \
            .getOrCreate()

        # Load CSV into DataFrame
        df = spark.read.csv(input_path, header=True)

        # Anonymize Columns
        for column in columns_to_anonymize:
            if column in df.columns:
                df = df.withColumn(column, sha2(col(column), 256))
            else:
                print(f"Warning: Column '{column}' not found in input data.")

        # Save the anonymized file
        df.write.csv(output_path, header=True, mode='overwrite')

        print(f"Anonymized data successfully written to: {output_path}")

    except Exception as e:
        print(f"Error during anonymization: {str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    # Example usage
    input_file = "/data/test.csv"
    output_file = "/data/anonymized_test.csv"
    columns = ["first_name", "last_name", "address"]

    anonymize_data(input_file, output_file, columns)
