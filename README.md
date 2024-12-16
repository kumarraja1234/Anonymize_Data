# Spark CSV Anonymizer

This project demonstrates anonymization of CSV datasets using PySpark, focusing on handling large datasets efficiently. It includes processes for:
- Generating sample data (`CSVGenerate.py`).
- Anonymizing small datasets (`test.csv`).
- Scaling up anonymization for large datasets (`large.csv`).

The project leverages Docker to set up a Spark environment and uses distributed computing to process large datasets on resource-constrained systems.

---

## **Project Structure**

```plaintext
project-directory/
│
├── Dockerfile                   # Docker configuration for Spark environment
├── docker-compose.yml           # Defines services for Spark master and worker
├── anonymize_large_data.py      # PySpark script for large dataset anonymization
├── anonymize_test_data.py       # PySpark script for small dataset anonymization
├── CSVGenerate.py               # Script to generate test and large CSV files
├── data/                        # Directory for input/output data
│   ├── test.csv                 # Sample test dataset (~1 million rows)
│   ├── large.csv                # Large dataset (~30 million rows)
│   ├── checkpoints              # Checkpoint directory for fault tolerance
│   ├── anonymized_test.csv      # Output of anonymized test data
│   ├── anonymized_large         # Output of anonymized large data
```

## Setup Instructions
1. Prerequisites
   - Docker and Docker Compose must be installed on your system.
   - Python libraries:
       - faker: To generate CSV data. Install using:
         ```bash
         pip install faker
         ```
2. Generate CSV Data
     - Use CSVGenerate.py to generate test and large datasets:
       ```bash
           python CSVGenerate.py
       ```
      - `test.csv`: A smaller dataset with 1 million rows for testing.
      - `large.csv`: A large dataset with 30 million rows for performance benchmarking.
        
3. Run Anonymization for Test Data
     - To test anonymization on a small dataset:
        1. Update the Dockerfile to use anonymize_test_data.py:
           ```bash
               CMD ["spark-submit", "/opt/bitnami/spark/anonymize_test_data.py"]
           ```
        2. Start the Docker container::
           ```bash
               docker-compose up
           ```
        3. Verify the anonymized output at data/anonymized_test.csv.

4. Run Anonymization for Large Data
    - To process the large dataset:
        1. Update the Dockerfile to use anonymize_large_data.py:
           ```bash
               CMD ["spark-submit", "/opt/bitnami/spark/anonymize_large_data.py"]
           ```
        2. Start the Spark services using Docker Compose:
           ```bash
               docker-compose up
           ```
        3. Run the Spark job:
           ```bash
               docker exec -it spark-master spark-submit \
              --input /data/large.csv \
              --output /data/anonymized_large \
              --columns first_name last_name address \
              --partitions 16 \
              --output-format parquet
           ```
         4. Verify the anonymized output in the data/anonymized_large folder.
     
## Features and Enhancements
  1. Configurations for Large Dataset
     - Partitions:
       - The dataset is repartitioned (--partitions 16) for efficient distributed processing.
     - Output Format:
       - Anonymized data is written in Parquet format (--output-format parquet) for optimal storage and processing.
     - Memory Management:
       - Worker memory is limited to 3.5G in docker-compose.yml to match system resources.
     - Logging:
       - Logs are implemented in anonymize_large_data.py to monitor progress and errors.
     - Checkpointing:
       - Fault tolerance is enabled using Spark checkpoints (/data/checkpoints).
       
  2. Exception Handling
      - The scripts handle errors such as missing columns and Spark execution failures.
      - Warnings are logged for columns not found in the input data.

## Workflow
 1. Generate Input Data:
    - Use CSVGenerate.py to create input CSV files (test.csv, large.csv).
 2. Test Anonymization:
    - Validate the pipeline on smaller datasets using anonymize_test_data.py.
 3. Scale for Large Data:
    - Modify configurations to handle the large dataset efficiently:
      - Adjust partitions and output-format.
      - Enable checkpointing.
 4. Run on Large Dataset:
    - Execute the Spark job using Docker and monitor progress via logs.

## Commands for Verification
### Verify Input and Output Data
1. List Input Files:
   ```bash
         ls data/
   ```
2. Check Anonymized Output: For small datasets:
   ```bash
           head -n 5 data/anonymized_test.csv
   ```
   For Large Datasets:
   ```bash
           docker exec -it spark-master ls /data/anonymized_large
   ```
### Monitor Spark Logs
Use docker logs to debug or monitor Spark processes:
   ```bash
           docker logs spark-master
           docker logs spark-worker
   ```

### Notes
   - The test dataset output (anonymized_test.csv) is in CSV format, while the large dataset output (anonymized_large) is in Parquet format for better performance.
   - Ensure adequate system resources (e.g., memory, CPU) for processing large datasets.

### Project Summary
This project demonstrates how to leverage PySpark for anonymizing sensitive data in large datasets. It transitions from testing on small datasets to scaling for larger data, addressing challenges like resource constraints, fault tolerance, and efficient storage. The use of Docker ensures portability and reproducibility across environments.
