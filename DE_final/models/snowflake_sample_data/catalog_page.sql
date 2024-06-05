with source_catalog_page as(
    select * from {{source('snowflake_sample_data','catalog_page')}}
),

final as (
    select * from source_catalog_page
)

select * from final