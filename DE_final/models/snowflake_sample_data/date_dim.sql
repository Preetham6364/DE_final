with source_date_dim as(
    select * from {{source('snowflake_sample_data','date_dim')}}
),

final as (
    select * from source_date_dim
)

select * from final