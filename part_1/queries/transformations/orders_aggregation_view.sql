-- View containing an aggregated view of purchases

create or replace view part_1.orders_aggregated as (
    select 
        o.order_id,
        o.customer_id, 
        o.product_id,
        min(o.order_purchase_timestamp) as order_purchase_timestamp, 
        sum(o.price + freight_value) as total_price, 
        count(o.order_id) as qty_of_items,
        o.product_category_name
    from part_1.orders o join part_1.payments p on o.order_id = p.order_id
    group by product_category_name, order_id, customer_id, product_id
);
