with source_store_returns as(
    select * from {{source('snowflake_sample_data','store_returns')}}
),

final as (
    select * from source_store_returns
)

select * from final