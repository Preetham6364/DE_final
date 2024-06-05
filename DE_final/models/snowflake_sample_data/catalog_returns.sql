with source_CATALOG_RETURNS as(
    select * from {{source('snowflake_sample_data','CATALOG_RETURNS')}}
),

final as (
    select * from source_CATALOG_RETURNS
)

select * from final