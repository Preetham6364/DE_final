with source_promotion as(
    select * from {{source('snowflake_sample_data','promotion')}}
),

final as (
    select * from source_promotion
)

select * from final