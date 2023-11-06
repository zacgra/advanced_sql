with 
    customer_name_and_address as (
        select
            c.customer_id,
            first_name || ' ' || last_name as customer_name,
            ca.customer_city,
            ca.customer_state
    from vk_data.customers.customer_address as ca
    join vk_data.customers.customer_data c 
        on ca.customer_id = c.customer_id
    ),
    
    customer_locations as (
        select 
            c.*,
            us.geo_location
        from customer_name_and_address as c
        left join vk_data.resources.us_cities as us 
            on UPPER(rtrim(ltrim(c.customer_state))) = upper(TRIM(us.state_abbr))
            and trim(lower(c.customer_city)) = trim(lower(us.city_name)) 
    ),

    affected_customers as (
        select
            c.*
        from customer_locations as c
        where 
            (  
                (( c.customer_state = 'KY') and (trim(c.customer_city) ilike '%concord%' 
                                            or trim(c.customer_city) ilike '%georgetown%' 
                                            or trim(c.customer_city) ilike '%ashland%' )
                )  
                or
                ( (c.customer_state = 'CA') and (trim(c.customer_city) ilike '%oakland%' 
                                                or trim(c.customer_city) ilike '%pleasant hill%' )
                )
                or
                ( (c.customer_state = 'TX') and (trim(c.customer_city) ilike '%arlington%'
                                            or trim(c.customer_city) ilike '%brownsville%') 
                )
            )
    ),

    chicago_geolocation as (
        select
            geo_location
        from vk_data.resources.us_cities 
        where city_name = 'CHICAGO' and state_abbr = 'IL'
    ),

    gary_geolocation as (
        select
            geo_location
        from vk_data.resources.us_cities 
        where city_name = 'GARY' and state_abbr = 'IN'
    ),

    affected_customers_with_suppy_store_distance as (
        select
            customer.*,
            (st_distance(customer.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles,
            (st_distance(customer.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
        from affected_customers as customer
        cross join chicago_geolocation as chicago
        cross join gary_geolocation as gary
    ),

    customer_preferences as (
        select 
            cs.customer_id,
            count(cs.customer_id) as food_pref_count
        from vk_data.customers.customer_survey as cs
        where is_active = true
        group by 1
    )
    
select
    c.customer_name,
    c.customer_city,
    c.customer_state,
    cp.food_pref_count,
    c.chicago_distance_miles,
    c.gary_distance_miles
from affected_customers_with_suppy_store_distance as c
join customer_preferences as cp
    on c.customer_id = cp.customer_id