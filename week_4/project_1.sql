/* Part 1 */

with
    customer_urgent_orders_ranked as (
        select
            c.c_custkey as customer_key,
            o.o_orderkey as order_key,
            o.o_orderdate as order_date,
            l.l_partkey as part_key,
            l.l_quantity as part_quantity,
            l.l_extendedprice as part_price,
            rank() over (partition by c.c_custkey order by part_price desc) as price_ranking
        from snowflake_sample_data.tpch_sf1.customer as c
        inner join snowflake_sample_data.tpch_sf1.orders as o
            on  c.c_custkey = o.o_custkey
        inner join snowflake_sample_data.tpch_sf1.lineitem as l
            on o.o_orderkey = l.l_orderkey
        where upper(o.o_orderpriority) = '1-URGENT'
            and upper(c.c_mktsegment) = 'AUTOMOBILE'
    ),

    highest_cost_orders as (
        select 
            cuor.customer_key,
            max(cuor.order_date) as last_order_date,
            listagg(cuor.order_key, ', ') as order_numbers,
            sum(cuor.part_price) as total_spent,
            max(case when price_ranking = 1 then part_key end) as part_1_key,
            max(case when price_ranking = 1 then part_quantity end) as part_1_quantity,
            max(case when price_ranking = 1 then part_price end) as part_1_total_cost,
            max(case when price_ranking = 2 then part_key end) as part_2_key,
            max(case when price_ranking = 2 then part_quantity end) as part_2_quantity,
            max(case when price_ranking = 2 then part_price end) as part_2_total_cost,
            max(case when price_ranking = 2 then part_key end) as part_3_key,
            max(case when price_ranking = 2 then part_quantity end) as part_3_quantity,
            max(case when price_ranking = 2 then part_price end) as part_3_total_cost
        from customer_urgent_orders_ranked as cuor
        where price_ranking <= 3
        group by cuor.customer_key
    )
 
select
    hco.customer_key,
    hco.last_order_date,
    hco.order_numbers,
    hco.total_spent,
    hco.part_1_key,
    hco.part_1_quantity,
    hco.part_1_total_cost,
    hco.part_2_key,
    hco.part_2_quantity,
    hco.part_2_total_cost,
    hco.part_3_key,
    hco.part_3_quantity,
    hco.part_3_total_cost
from highest_cost_orders as hco
order by hco.last_order_date desc
limit 100


/*
Part 2

a. Honestly, I'm not really sure.  My resulting data set was different from the snippet in the assignment, and it seemed like Sravan's in the walkthrough was as well, but only in the order of the customer keys, as the order_numbers did match up.  However, the candidate's sample had a different order to their order_numbers.

b. The candidate used two ctos, which helped break up the logic a bit.  However, the urgent_orders as a high number of joins that are repeated in the final select.

c. Some things the candidate could do to improve things would be to reduce joins, as well as avoid ordering when it isn't useful, such as in the top_orders cto when they order by c_custkey.  Since that ordering isn't used in a window function and the final ordering is by last_order_date, it is just a performance hit.

/*
