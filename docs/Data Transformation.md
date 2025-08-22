# Data Transformation & Consumption

In order to transform data in the cloud based on the raw data previously ingested, the following workflow was designed:

![Data Transformation Workflow](image-11.png)

## Considerations

- The source data is the data that was previously ingested.
- We continue to use Snowflake as our cloud data platform.
- Airflow is currently deployed locally under the open source Astronomer distribution due to its ease of deployment and administration. It could be deployed on EC2 or Astronomer Cloud.
- Dbt is deployed containerized in Docker within Airflow, using the dbt Core distribution with the dbt connector to Snowflake. This allows for an isolated environment that is easy to deploy. Dbt is a very powerfull tool to execute sql commands, macros, testing and document data models.
- To generate DAGs that orchestrate DBT models (run + test) instead of using BatchOperators (which are more complex to manage in terms of initialization, configuration, and generate dependencies), we use the [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/getting_started/index.html) open source library, which internally manages and orchestrate dbt workflows easily.

## Architecture

- Since we are using Airflow to orchestrate, the architecture pattern, it is in batch mode, based on a cron schedule (or, if desired, a trigger could be generated from the Airflow API after data is loaded in raw layer).
- Now we have two more layers: **TRANSFORM**, which contains a clean and standardized data model (deduplication, data formatting, among other light transformations), and **CONSUMPTION**, which contains an enriched and intelligent data model (complex calculations, joins, fact/dims modeling, among others) for analytical purposes.

## Implementation

### Requirements
- Configure the connection profile between dbt and Snowflake correctly.
    - Airflow generic connection poiting to snowflake account must be configured.
- Dbt project config and Dbt execution config must be configured correctly.

A dag called [dbt_pipeline.py](../dags/dbt_pipeline.py) was created to orchestrate the execution of the dbt models (transform and consumption) to execute the run (load data) and test (unit testing).

![alt text](image-1.png)


### Transform layer

0. Define all the sources needed in [_raw.yml](../dags/dbt/novicap/models/raw/_raw.yml)

1. The SQL logic for the data model in transform was created in the file [stg_bank_loan](../dags/dbt/novicap/models/transform/stg_bank_loans.sql)
    
    - In the raw data, we have completely duplicate rows, which we delete in this step.
    - We converted some NA and N/A values in some columns to null.
    - We group together certain homogeneous categorical values to avoid redundancy.
    - We adjusted the data type for some columns.
    - We created a data completeness metric because after removing duplicate records, there is more than one record associated with the same loan_id and customer_id. The difference is that some of the records have more complete data than others. This completeness metric allows us to identify the best record by load_id and customer_id.

The resulting physical model is as follows

| name                          | type                |
|-------------------------------|---------------------|
| LOAN_ID                       | VARCHAR(50)         |
| CUSTOMER_ID                   | VARCHAR(50)         |
| LOAN_STATUS                   | VARCHAR(20)         |
| CURRENT_LOAN_AMOUNT           | NUMBER(38,0)        |
| TERM                          | VARCHAR(10)         |
| CREDIT_SCORE                  | NUMBER(38,0)        |
| ANUAL_INCOME                  | NUMBER(38,0)        |
| YEARS_IN_CURRENT_JOB          | VARCHAR(10)         |
| HOME_OWNERSHIP                | VARCHAR(20)         |
| PURPOSE                       | VARCHAR(16777216)   |
| MONTHLY_DEBT                  | FLOAT               |
| YEARS_CREDIT_HISTORY          | FLOAT               |
| MONTHS_SINCE_LAST_DELINQUENT  | NUMBER(38,0)        |
| NUM_OPEN_ACCOUNTS             | NUMBER(38,0)        |
| NUM_CREDIT_PROBLEMS           | NUMBER(38,0)        |
| CURRENT_CREDIT_BALANCE        | NUMBER(38,0)        |
| MAX_OPEN_CREDIT               | NUMBER(38,0)        |
| BANKRUPTCIES                  | NUMBER(38,0)        |
| TAX_LIENS                     | NUMBER(38,0)        |
| LOADED_AT                     | TIMESTAMP_NTZ(9)    |
| SOURCE_FILE                   | VARCHAR(500)        |
| COMPLETENESS_SCORE            | NUMBER(19,0)        |
| LOADED_AT_TRANS               | TIMESTAMP_NTZ(9)    |

2. Document transform data model in [_transform.yml](../dags/dbt/novicap/models/transform/_transform.yml)

    * Note: Due to time constraints, not all unit tests on the columns were defined to measure the quality of the resulting data.

3. The execution of the DBT model looks something like the following from the log in Airflow.

![alt text](image-12.png)

4. A preview of the data in Snowflake looks like this (As you can see, we went from 100k records to 82k)

![alt text](image-14.png)


Author: @svargasg [Sebastian Vargas Gantiva]

<div align="left">
    <a href="../docs/Data Ingestion.md"><< Data Ingestion</a>
</div>

<div align="right">
    <a href="../README.md">README >></a>
</div>