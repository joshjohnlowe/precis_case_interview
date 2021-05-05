-- 1. Aggregate orders table by grouping orders with the same order_id together, but keeping distinct product categories seperate
-- 2. Select the category name, and number of purchases within this category that contain the minimum timestamp for that customer


create or replace view part_1.top_first_purchases as (
    with orders_aggregated as (
        select 
            o.order_id,
            o.customer_id, 
            o.product_id,
            min(o.order_purchase_timestamp) as order_purchase_timestamp, 
            p.payment_value as total_paid,
            sum(o.price + o.freight_value) as total_cost, 
            count(o.order_id) as qty_of_items,
            o.product_category_name
        from part_1.orders o left join part_1.payments p on o.order_id = p.order_id
        group by product_category_name, order_id, customer_id, product_id, total_paid
    )
    select 
        product_category_name,
        count(1) as purchase_count
    from (
        select o.order_id, o.customer_id, o.order_purchase_timestamp, o.product_category_name
        from orders_aggregated o 
    inner join(
        select 
            customer_id,
            min(order_purchase_timestamp) as first_purchase,
        from orders_aggregated
        group by customer_id) x
    on o.customer_id = x.customer_id and o.order_purchase_timestamp = x.first_purchase)
    group by product_category_name
);


-- All orders tied to a given customer_id have an identical timestamp?
select distinct customer_id from part_1.orders; --98666  
select distinct customer_id, order_purchase_timestamp from part_1.orders; -- 98666