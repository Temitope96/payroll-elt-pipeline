{{ config(materialized='table') }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

employees as (
    select * from {{ ref('stg_employees') }}
),

periods as (
    select * from {{ ref('stg_pay_periods') }}
),

enriched as (
    select
        t.transaction_id,
        t.employee_id,
        e.full_name,
        e.country_code,
        e.department,
        e.salary_usd                    as annual_salary_usd,
        t.period_id,
        p.period_year,
        p.period_month,
        p.pay_date,
        t.gross_pay_usd,
        t.deductions_usd,
        t.net_pay_usd,
        t.currency_code,
        t.status,
        t.processed_at,
        case
            when t.status = 'PROCESSED' then true
            else false
        end                             as is_successful
    from transactions t
    left join employees e
        on t.employee_id = e.employee_id
    left join periods p
        on t.period_id = p.period_id
)

select * from enriched