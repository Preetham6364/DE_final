with source_customer as(
    select * from {{source('snowflake_sample_data','customer')}}
),

final as (
    select * from source_customer
)

select * from final