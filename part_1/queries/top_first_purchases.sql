create or replace view part_1.top_first_purchases as (
    select 
        product_category_name,
        count(1) as purchase_count
    from (
    select o.order_id, o.customer_id, o.order_purchase_timestamp, o.product_category_name
    from part_1.orders o 
    inner join
    (select 
        customer_id,
        min(order_purchase_timestamp) as first_purchase,
    from part_1.orders
    group by customer_id) x
    on o.customer_id = x.customer_id and o.order_purchase_timestamp = x.first_purchase
    )
    group by product_category_name
)