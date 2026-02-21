{{ config(materialized='table') }}

SELECT
    order_id,
    CAST(order_date AS DATE) as order_date,
    product_id,
    product_category,
    CAST(price AS DECIMAL(10,2)) as price,
    CAST(discount_percent AS FLOAT) / 100 as discount_percent,
    quantity_sold,
    customer_region,
    payment_method,
    rating,
    review_count,
    CAST(discounted_price AS DECIMAL(10,2)) as discounted_price,
    CAST(total_revenue AS DECIMAL(10,2)) as total_revenue
FROM
    public.raw_amazon_sales