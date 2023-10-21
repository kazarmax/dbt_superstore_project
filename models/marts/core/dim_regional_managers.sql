with regional_managers as (
    select
         manager_name
        ,r.region_id as region_id

    FROM {{ ref('stg_sales_managers') }} sm 
    JOIN {{ ref('dim_regions') }} r ON sm.region = r.region_name
)

select
    1000 + ROW_NUMBER() OVER(order by null) as id
    ,*
from regional_managers 