with source_call_center as(
    select * from {{source('snowflake_sample_data','call_center')}}
),

final as (
    select * from source_call_center
)

select * from final