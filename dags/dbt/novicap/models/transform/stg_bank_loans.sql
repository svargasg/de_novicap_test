with base_dedup as (
    select distinct * 
    from {{ source('RAW','BANK_LOANS') }}
    qualify rank() over (partition by loan_id, customer_id order by loaded_at desc) = 1
)
, base_formated as (
    select
        loan_id
        , customer_id
        , loan_status
        , current_loan_amount -- 99999999 values looks like dummy because loan_status is Fully Paid for all rows
        , term
        , credit_score
        , anual_income
        , nullif(years_in_current_job,'n/a') as years_in_current_job
        , case
            when home_ownership in ('Home Mortgage', 'HaveMortgage') then 'Home Mortgage' else home_ownership
            end as home_ownership
        , initcap( 
            replace(
                regexp_replace(lower(purpose), '[^a-z0-9 ]', ' '),
                '_', ' '
            )
        ) as purpose
        , monthly_debt
        , years_credit_history
        , nullif(months_since_last_delinquent,'NA')::int as months_since_last_delinquent
        , num_open_accounts
        , num_credit_problems
        , current_credit_balance
        , max_open_credit
        , nullif(bankruptcies,'NA')::int as bankruptcies
        , nullif(tax_liens,'NA')::int as tax_liens
        , loaded_at
        , source_file
    from base_dedup
)
, dedup_completeness as (
    select *,
        (case when loan_id is not null then 1 else 0 end
        + case when customer_id is not null then 1 else 0 end
        + case when loan_status is not null then 1 else 0 end
        + case when current_loan_amount is not null then 1 else 0 end
        + case when term is not null then 1 else 0 end
        + case when credit_score is not null then 1 else 0 end
        + case when anual_income is not null then 1 else 0 end
        + case when years_in_current_job is not null then 1 else 0 end
        + case when home_ownership is not null then 1 else 0 end
        + case when purpose is not null then 1 else 0 end
        + case when monthly_debt is not null then 1 else 0 end
        + case when years_credit_history is not null then 1 else 0 end
        + case when months_since_last_delinquent is not null then 1 else 0 end
        + case when num_open_accounts is not null then 1 else 0 end
        + case when num_credit_problems is not null then 1 else 0 end
        + case when current_credit_balance is not null then 1 else 0 end
        + case when max_open_credit is not null then 1 else 0 end
        + case when bankruptcies is not null then 1 else 0 end
        + case when tax_liens is not null then 1 else 0 end) as completeness_score
    from base_formated
    qualify row_number() over (partition by loan_id, customer_id order by completeness_score desc) = 1
)
select *
    , convert_timezone('UTC',current_timestamp())::TIMESTAMP_NTZ as loaded_at_trans
from dedup_completeness
