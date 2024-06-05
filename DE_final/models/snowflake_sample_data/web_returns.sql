with source_web_returns as(
    select * from {{source('snowflake_sample_data','web_returns')}}
),

final as (
    select * from source_web_returns
)

select * from final