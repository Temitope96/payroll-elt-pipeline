{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'pay_periods') }}
),

renamed as (
    select
        period_id,
        period_start::date       as period_start,
        period_end::date         as period_end,
        pay_date::date           as pay_date,
        period_year::int         as period_year,
        period_month::int        as period_month,
        _loaded_at
    from source
    where period_id is not null
)

select * from renamed