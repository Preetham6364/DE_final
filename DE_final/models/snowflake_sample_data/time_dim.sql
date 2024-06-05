with source_time_dim as(
    select * from {{source('snowflake_sample_data','time_dim')}}
),

final as (
    select * from source_time_dim
)

select * from final