CREATE DATABASE NOVICAP_DE;

CREATE SCHEMA NOVICAP_RAW;

create or replace table novicap_raw.bank_loans (
    loan_id varchar(50),
    customer_id varchar(50),
    loan_status varchar(20),
    current_loan_amount bigint,
    term varchar(10),
    credit_score integer,
    anual_income bigint,
    years_in_current_job varchar(10),
    home_ownership varchar(20),
    purpose varchar(20),
    monthly_debt float,
    years_credit_history float,
    months_since_last_delinquent varchar(5),
    num_open_accounts integer,
    num_credit_problems integer,
    current_credit_balance bigint,
    max_open_credit bigint,
    bankruptcies varchar(5),
    tax_liens varchar(5),
    loaded_at timestamp_ntz default convert_timezone('UTC',current_timestamp())::TIMESTAMP_NTZ,
    source_file varchar(500)
);

-- ensure that you have a IAM Role with enough permissions to s3 where you will receive data

create or replace storage integration s3_integration
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::412885113237:role/snowflake-s3-access-role'
  storage_allowed_locations = ('s3://novi-raw-dev/');

-- execute command below to obtain STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID and replace with those values in IAM Role snowflake-s3-access-role trust entities.
-- DESC INTEGRATION s3_integration;

create or replace stage novicap_raw.bank_loans_stage
  url='s3://novi-raw-dev/data/'
  storage_integration = s3_integration
  file_format = (type = csv skip_header=1 trim_space=TRUE);

-- validate if you can see s3 files with list command below
-- list @novicap_raw.bank_loans_stage;

create or replace pipe novicap_raw.bank_loans_pipe
  auto_ingest = true
as
copy into novicap_raw.bank_loans
from (
    select
        t.$1::varchar as loan_id,
        t.$2::varchar as customer_id,
        t.$3::varchar as loan_status,
        t.$4::bigint as current_loan_amount,
        t.$5::varchar as term,
        t.$6::integer as credit_score,
        t.$7::bigint as anual_income,
        t.$8::varchar as years_in_current_job,
        t.$9::varchar as home_ownership,
        t.$10::varchar as purpose,
        t.$11::float as monthly_debt,
        t.$12::float as years_credit_history,
        t.$13::varchar as months_since_last_delinquent,
        t.$14::integer as num_open_accounts,
        t.$15::integer as num_credit_problems,
        t.$16::bigint as current_credit_balance,
        t.$17::bigint as max_open_credit,
        t.$18::varchar as bankruptcies,
        t.$19::varchar as tax_liens,
        convert_timezone('UTC',current_timestamp())::TIMESTAMP_NTZ as loaded_at,
        metadata$filename as source_file
    from @novicap_raw.bank_loans_stage t
)
file_format = (type = csv skip_header=1 trim_space=TRUE);

-- execute command below to obtain notification_channel (arn sqs)
-- desc pipe novicap_raw.bank_loans_pipe;

-- configure notification event from s3 to aws sqs used in snowpipe (previous arn sqs)
-- aws s3api put-bucket-notification-configuration --bucket novi-raw-dev --notification-configuration "{\"QueueConfigurations\":[{\"QueueArn\":\"arn:aws:sqs:us-east-1:582054875686:sf-snowpipe-AIDAYPBJMSITB32MCT25X-2xOum0v0ILTBPi6sxsvAqg\",\"Events\":[\"s3:ObjectCreated:*\"],\"Filter\":{\"Key\":{\"FilterRules\":[{\"Name\":\"prefix\",\"Value\":\"data/\"},{\"Name\":\"suffix\",\"Value\":\".csv\"}]}}}]}"

-- upload a .csv file in the data/ folder and wait until pipe loads the data
-- select * from novicap_raw.bank_loans;
