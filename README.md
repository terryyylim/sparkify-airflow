# sparkify-airflow

## Content
* [Summary](#Summary)
* [ETL](#ETL)
* [Project structure](#Project-structure)
* [Installation](#Installation)

### Summary
This project involves the use of [S3](https://aws.amazon.com/en/s3/) (Data storage) and Airflow.

Data sources are provided by the public ``S3 buckets``. The first bucket contains information about songs and artists, while the second bucket contains simulated app activity logs by users. The objects contained in both buckets <br> are JSON files.

#### Configuring the DAG
In the DAG, add default parameters according to these guidelines
* The DAG does not have dependencies on past runs
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Do not email on retry

### Project Structure
1. sparkify_dag.py
* Has all the imports and task dependencies set
2. operators (folder)
* Contains helper operators that are derived from BaseOperator to perform/trigger certain tasks synchronously
3. plugins (folder)
* Contains helper class for SQL transformations
4. dwh-example.cfg
* Example format for configurations required

### Installation
Clone this repository:
```
git clone https://github.com/terryyylim/sparkify-airflow.git
```

Change to sparkify-airflow directory
```
cd sparkify-airflow
```

To prevent complications to the global environment variables, I suggest creating a virtual environment for tracking and using the libraries required for the project.

1. Create and activate a virtual environment (run `pip3 install virtualenv` first if you don't have Python virtualenv installed):
```
virtualenv -p python3 <desired-path>
source <desired-path>/bin/activate
```

2. Install the requirements:
```
pip install -r requirements.txt
```