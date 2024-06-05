with source_web_site as(
    select * from {{source('snowflake_sample_data','web_site')}}
),

final as (
    select * from source_web_site
)

select * from final