# Geolocation Data Processing Pipeline

## Description
This project implements a data processing workflow to analyze geolocation data and generate daily activity reports for specific postal codes. The pipeline integrates demographic data to compute basic statistics such as the average age of visitors per postal code.

---

## Installation and Environment Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/ivanhn1/mytraffic
   cd geolocation_data_pipeline
Set Up Virtual Environment:

# Geolocation Data Processing Pipeline

## Description
This project implements a data processing pipeline to analyze geolocation data, clean it, and generate daily activity reports for specific postal codes. Additionally, it computes demographic statistics, such as the average age of visitors per postal code. The pipeline is built using PySpark and orchestrated to run daily using Apache Airflow.

---

## Table of Contents
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup Instructions](#setup-instructions)
- [Data Files](#data-files)
- [Execution](#execution)
  - [Local Execution](#local-execution)
  - [Execution with Airflow](#execution-with-airflow)
- [Output](#output)
- [Validation](#validation)
- [Proposed Improvements](#proposed-improvements)
- [Contact](#contact)

---

## Project Structure

The project is organized as follows:

```plaintext
geolocation_data_pipeline/
├── dags/
│   ├── workflow_dag.py         # Airflow DAG definition
├── data/
│   ├── signals.csv             # Input geolocation data
│   ├── sociodemographics.csv   # Input demographic data
│   ├── postal_codes.csv        # Postal code mappings
│── output/                 # Directory for Parquet outputs
├── spark_jobs/
│   ├── process_geolocation.py  # PySpark job for data processing
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation


Create and activate a virtual environment:
python3 -m venv venv
source venv/bin/activate
Upgrade pip:
pip install --upgrade pip

Install Dependencies:
pip install -r requirements.txt

Configure Apache Airflow:
Initialize the Airflow database:
airflow db init
Create an admin user for the Airflow UI:
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
Place Input Data: Ensure the following files are placed in the data directory:

signals.csv: Geolocation data.
sociodemographics.csv: Demographic data.
postal_codes.csv: A file mapping latitude and longitude ranges to postal codes.
Steps to Run the Solution
Local Execution
To execute the data processing pipeline locally:

Run the PySpark script directly:
python spark_jobs/process_geolocation.py

Execution with Airflow
Copy the DAG file to the Airflow DAGs directory:
cp dags/workflow_dag.py $AIRFLOW_HOME/dags/
Start the Airflow web server and scheduler:
airflow webserver &
airflow scheduler &
Access the Airflow UI at http://localhost:8080.

Enable the DAG named geolocation_pipeline and trigger it manually or let it run as scheduled.

Explanation of Geolocation Mapping and Integration
Geolocation Mapping
The pipeline maps geolocation coordinates (latitude and longitude) to postal codes using the provided postal_codes.csv file. The file defines a range of latitude and longitude values for each postal code. The mapping is performed as follows:

Each geolocation entry is checked to see if its latitude and longitude fall within the defined range (lat_min, lat_max, long_min, long_max) for any postal code.
If a match is found, the postal code is assigned to the geolocation record.

Integration with Demographic Data
After mapping geolocation data to postal codes, the pipeline joins the resulting dataset with demographic data (sociodemographics.csv) on the device_id field. This enables the enrichment of geolocation data with demographic attributes such as age, gender, and income.

The joined dataset is then aggregated to compute:

Visit Count: The total number of visits per postal code and date.
Average Age: The average age of visitors to each postal code.
