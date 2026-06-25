{{ config(materialized='view') }}

with source as (
    select * from {{ source('staging', 'employees') }}
),

renamed as (
    select
        employee_id,
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,
        lower(email)                     as email,
        upper(country_code)              as country_code,
        department,
        hire_date::date                  as hire_date,
        salary_usd::numeric(12,2)        as salary_usd,
        is_active::boolean               as is_active,
        created_at::timestamp            as created_at,
        _loaded_at
    from source
    where employee_id is not null
)

select * from renamed