-- I made heavy use of CTEs to keep from confusing myself.  I tried to limit transformations so that each CTE only
-- contained one step's transformations.  I knew I wanted to compare geolocation of customer and supplier, 
-- so I made to two tables with the geolocation of customer and supplier using the unique cities.  

-- Approach
-- 1: get unique cities
-- 2: add geolocation to suppliers using unique cities
-- 3: add geolocation to customers using unique cities
-- 4: get distance between customer and each supplier (with cross join)
-- 5: rank distances for each customer to all suppliers
-- 6: return the closest result ordered by first/last name

with unique_cities
    as (select distinct
            uc.city_name,
            uc.state_abbr,
            uc.geo_location
        from vk_data.resources.us_cities uc
        ),

    supplier_locations
        as ( 
            select
                si.supplier_id,
                si.supplier_name,
                si.supplier_city,
                si.supplier_state,
                uc.geo_location as supplier_geolocation
            from vk_data.resources.us_cities as uc
            join vk_data.suppliers.supplier_info as si
                on lower(uc.city_name) = lower(si.supplier_city) 
                and lower(uc.state_abbr) = lower(si.supplier_state)
        ),
        
    customer_locations
        as (
            select
                ca.address_id,
                ca.customer_id,
                ca.customer_city,
                ca.customer_state,
                uc.geo_location as customer_geolocation
            from unique_cities as uc
            join vk_data.customers.customer_address as ca
                on lower(trim(uc.city_name)) = lower(trim(ca.customer_city)) 
                and lower(trim(uc.state_abbr)) = lower(trim(ca.customer_state))
        ),
        
    customer_to_supplier_distances
        as (
            select 
                *,
                round(st_distance(sl.supplier_geolocation, cl.customer_geolocation) / 1609, 2) as distance_in_miles
            from supplier_locations as sl
            join customer_locations as cl -- does using a join without the on key indicate a cross product?
        ),
        
    nearest_suppliers_ranked
        as (
            select
                *,
                row_number() over (partition by customer_id order by distance_in_miles) as store_rank
            from customer_to_supplier_distances
        )
select
    cd.customer_id,
    cd.first_name,
    cd.last_name,
    customer_city,
    customer_state,
    supplier_id,
    supplier_name,
    supplier_city,
    supplier_state,
    distance_in_miles
from nearest_suppliers_ranked nsr
join vk_data.customers.customer_data cd
    on  nsr.customer_id = cd.customer_id
where store_rank = 1
order by 3,2