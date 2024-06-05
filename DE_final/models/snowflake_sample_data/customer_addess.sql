with source_customer_address as(
    select * from {{source('snowflake_sample_data','customer_address')}}
),

final as (
    select * from source_customer_address
)

select * from final