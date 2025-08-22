with refined_bl as (
    select
        loan_id || '|' || customer_id as id
        , loan_status
        , current_loan_amount
        , term
        , coalesce(
            coalesce(
            credit_score, 
            mode(credit_score) over (partition by loan_status, term, home_ownership, purpose)
            ),
            mode(credit_score) over (partition by null)
        ) as credit_score
        , coalesce(
            coalesce(
            anual_income, 
            avg(anual_income) over (partition by loan_status, term, home_ownership, purpose)
            ),
            avg(anual_income) over (partition by null)
        )::bigint as anual_income
        , coalesce(
            coalesce(
            years_in_current_job, 
            mode(years_in_current_job) over (partition by loan_status, term, home_ownership, purpose)
            ),
            mode(years_in_current_job) over (partition by null)
        ) as years_in_current_job
        , home_ownership
        , purpose
        , monthly_debt
        , years_credit_history
        , num_open_accounts
        , num_credit_problems
        , current_credit_balance
        , max_open_credit
        , bankruptcies
        , tax_liens
    from {{ ref('stg_bank_loans') }}
    where true 
        and ( 
            max_open_credit is not null
            and bankruptcies is not null
            and tax_liens is not null
        )
)
, ohe_loan_status as (
    SELECT *
    FROM (
        SELECT id, loan_status, 1 AS dummy 
        FROM refined_bl
        )
    PIVOT (
        MAX(dummy) FOR loan_status IN (SELECT loan_status FROM refined_bl) 
        DEFAULT ON NULL (0)
        )
)
, ohe_term as (
    SELECT *
    FROM (
        SELECT id, term, 1 AS dummy 
        FROM refined_bl
        )
    PIVOT (
        MAX(dummy) FOR term IN (SELECT term FROM refined_bl) 
        DEFAULT ON NULL (0)
        )
)
, ohe_yicj as (
    SELECT *
    FROM (
        SELECT id, years_in_current_job, 1 AS dummy 
        FROM refined_bl
        )
    PIVOT (
        MAX(dummy) FOR years_in_current_job IN (SELECT years_in_current_job FROM refined_bl) 
        DEFAULT ON NULL (0)
        )
)
, ohe_home_ownership as (
    SELECT *
    FROM (
        SELECT id, home_ownership, 1 AS dummy 
        FROM refined_bl
        )
    PIVOT (
        MAX(dummy) FOR home_ownership IN (SELECT home_ownership FROM refined_bl) 
        DEFAULT ON NULL (0)
        )
)
, ohe_purpose as (
    SELECT *
    FROM (
        SELECT id, purpose, 1 AS dummy 
        FROM refined_bl
        )
    PIVOT (
        MAX(dummy) FOR purpose IN (SELECT purpose FROM refined_bl) 
        DEFAULT ON NULL (0)
        )
)
select
    t1.current_loan_amount
    , t1.credit_score
    , t1.anual_income
    , t1.monthly_debt
    , t1.years_credit_history
    , t1.num_open_accounts
    , t1.num_credit_problems
    , t1.current_credit_balance
    , t1.max_open_credit
    , t1.bankruptcies
    , t1.tax_liens
    , t2.* exclude(id)
    , t3.* exclude(id)
    , t4.* exclude(id)
    , t5.* exclude(id)
    , t6.* exclude(id)
from refined_bl t1
inner join ohe_loan_status t2
on t1.id = t2.id
inner join ohe_term t3
on t1.id = t3.id
inner join ohe_yicj t4
on t1.id = t4.id
inner join ohe_home_ownership t5
on t1.id = t5.id
inner join ohe_purpose t6
on t1.id = t6.id
