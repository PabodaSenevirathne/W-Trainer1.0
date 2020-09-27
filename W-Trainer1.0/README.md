# W-Trainer1.0

W-Trainer1.0 is data pipeline that have series of processes extracting data from one system, transforming it, and loading into a Snowflake database.
The steps used to develop  W-Trainer1.0 data pipeline includes,
1.	Extract
2.	Transform
3.	Load

## Getting Started
The following instructions will help to set up the project and run the ETL pipeline.

### Prerequisites

Use the requirements.txt file in order to install required python packages.
pip install -r requirements.txt

```
pip install -r requirements.txt
```
## Built With

* [snowflake](https://www.snowflake.com/) - The Cloud Data Platform 

## Instructions for running locally

Step 1:
Clone repository to local machine

Step 2:
Change directory to local repository

Step 3:
Create Python Virtual Environment and install necessary Python packages using requirements.txt file. 

Step 4:
Run scripts
```
cd src/
python -m etl.py
```
