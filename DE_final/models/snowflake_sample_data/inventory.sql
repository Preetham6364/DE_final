with source_inventory as(
    select * from {{source('snowflake_sample_data','inventory')}}
),

final as (
    select * from source_inventory
)

select * from final