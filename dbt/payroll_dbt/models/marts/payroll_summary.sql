{{ config(materialized='table') }}

with base as (
    select * from {{ ref('int_payroll_enriched') }}
    where is_successful = true
),

summary as (
    select
        period_year,
        period_month,
        country_code,
        department,
        count(distinct employee_id)     as headcount,
        count(transaction_id)           as transaction_count,
        sum(gross_pay_usd)              as total_gross_usd,
        sum(deductions_usd)             as total_deductions_usd,
        sum(net_pay_usd)                as total_net_usd,
        avg(net_pay_usd)                as avg_net_pay_usd,
        min(net_pay_usd)                as min_net_pay_usd,
        max(net_pay_usd)                as max_net_pay_usd
    from base
    group by 1, 2, 3, 4
)

select * from summary