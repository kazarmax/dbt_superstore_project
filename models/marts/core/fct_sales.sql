with sales as (
    select
         order_id
        ,to_char(order_date, 'yyyymmdd')::int AS order_date_id
        ,to_char(ship_date, 'yyyymmdd')::int AS ship_date_id
        ,sales
        ,profit
        ,quantity
        ,discount
        ,products.id as dim_products_id
        ,customers.id as dim_customers_id
        ,shipping.id as dim_shipping_id
        ,geo.id as dim_geo_id

    from {{ ref('stg_orders') }} as orders

    join {{ ref('dim_products') }} as products on 
        products.product_id = orders.product_id
        and products.product_name = orders.product_name
        and products.product_category = orders.product_category
        and products.product_subcategory = orders.product_subcategory
        and products.segment = orders.segment

    join {{ ref('dim_customers') }} as customers on 
        customers.customer_id = orders.customer_id

    join {{ ref('dim_shipping') }} as shipping on
        shipping.ship_mode = orders.ship_mode
    
    join {{ ref('dim_geo') }} as geo on
        geo.country = orders.country
        and geo.city = orders.city
        and geo.state = orders.state 
        and geo.postal_code = orders.postal_code
)

SELECT
	100 + ROW_NUMBER() over(order by null) AS id
    , *
from sales
