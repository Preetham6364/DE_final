with source_web_page as(
    select * from {{source('snowflake_sample_data','web_page')}}
),

final as (
    select * from source_web_page
)

select * from final