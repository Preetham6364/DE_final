with source_catalog_sales as(
    select * from {{source('snowflake_sample_data','catalog_sales')}}
),

final as (
    select * from source_catalog_sales
)

select * from final