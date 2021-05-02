create or replace view part_1.high_paying_customers as(
select(
select count(1) from (
select 
    avg(p.payment_value) as average_payment,
    c.customer_id 
from 
    part_1.payments p join 
    part_1.orders o on o.order_id = p.order_id join 
    part_1.customers c on c.customer_id = o.customer_id
where p.currency = 'SEK'
group by c.customer_id
) X where X.average_payment > 200
) as customers_paying_over_200,
(select count(1) from part_1.customers) as total_customers
);