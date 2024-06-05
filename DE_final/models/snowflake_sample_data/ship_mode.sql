with source_ship_mode as(
    select * from {{source('snowflake_sample_data','ship_mode')}}
),

final as (
    select * from source_ship_mode
)

select * from final