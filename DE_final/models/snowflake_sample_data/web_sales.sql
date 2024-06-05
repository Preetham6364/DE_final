with source_web_sales as(
    select * from {{source('snowflake_sample_data','web_sales')}}
),

final as (
    select * from source_web_sales
)

select * from final