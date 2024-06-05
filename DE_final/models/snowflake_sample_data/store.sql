with source_store as(
    select * from {{source('snowflake_sample_data','store')}}
),

final as (
    select * from source_store
)

select * from final