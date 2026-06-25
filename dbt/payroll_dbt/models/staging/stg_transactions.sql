{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        employee_id,
        period_id,
        gross_pay_usd::numeric(12,2)    as gross_pay_usd,
        deductions_usd::numeric(12,2)   as deductions_usd,
        net_pay_usd::numeric(12,2)      as net_pay_usd,
        upper(currency_code)            as currency_code,
        upper(status)                   as status,
        processed_at::timestamp         as processed_at,
        _loaded_at
    from source
    where transaction_id is not null
)

select * from renamed