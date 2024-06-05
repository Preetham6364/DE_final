with source_customer_demographics as(
    select * from {{source('snowflake_sample_data','customer_demographics')}}
),

final as (
    select * from source_customer_demographics
)

select * from final