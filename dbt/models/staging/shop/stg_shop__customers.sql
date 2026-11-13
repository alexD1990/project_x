with source as (

    select * from {{ source('shop', 'customers') }}

),

latest as (

    -- The source keeps old versions of a customer; keep the newest one.
    select *
    from source
    qualify row_number() over (partition by customer_id order by updated_at desc) = 1

),

renamed as (

    select
        customer_id,
        initcap(trim(first_name))           as first_name,
        initcap(trim(last_name))            as last_name,
        lower(trim(email))                  as email,
        upper(country_code)                 as country_code,
        city,
        signup_date,
        coalesce(marketing_opt_in, false)   as is_marketing_opt_in,
        updated_at
    from latest

)

select * from renamed
