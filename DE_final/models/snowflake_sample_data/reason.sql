with source_reason as(
    select * from {{source('snowflake_sample_data','reason')}}
),

final as (
    select * from source_reason
)

select * from final