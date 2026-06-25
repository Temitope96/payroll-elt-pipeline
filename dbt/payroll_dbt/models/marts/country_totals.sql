{{ config(materialized='table') }}

with base as (
    select * from {{ ref('int_payroll_enriched') }}
    where is_successful = true
),

totals as (
    select
        country_code,
        period_year,
        count(distinct employee_id)     as total_employees,
        count(transaction_id)           as total_transactions,
        sum(gross_pay_usd)              as total_gross_usd,
        sum(net_pay_usd)                as total_net_usd,
        avg(net_pay_usd)                as avg_net_pay_usd,
        sum(gross_pay_usd) - 
        sum(net_pay_usd)                as total_deductions_usd
    from base
    group by 1, 2
)

select * from totals