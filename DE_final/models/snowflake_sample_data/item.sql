with source_item as(
    select * from {{source('snowflake_sample_data','item')}}
),

final as (
    select * from source_item
)

select * from final