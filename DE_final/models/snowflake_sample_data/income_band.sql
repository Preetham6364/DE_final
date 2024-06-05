with source_income_band as(
    select * from {{source('snowflake_sample_data','income_band')}}
),

final as (
    select * from source_income_band
)

select * from final