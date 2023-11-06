with     
    customer_name_and_address as (
        /*    
            consolidates pre-query customer data
        */

        select
            c.customer_id,
            first_name || ' ' || last_name as customer_name,
            ca.customer_city,
            ca.customer_state
        from vk_data.customers.customer_address as ca
        join vk_data.customers.customer_data c 
            on ca.customer_id = c.customer_id
    ),
    
    affected_customers as (
        /*  - filters out unaffected customers
            - passes the prior related CTEs records along for accumulating results
              so that we don't recalculate CTEs multiple times
            - since ilike matches case insensitive, we don't need to worry about
              case.  Also, our wildcards will match regardless of whitespace
              before or after our target string so we don't need trim.
        */

        select
            c.*
        from customer_name_and_address as c
        where 
            (  
                (( c.customer_state = 'KY') and (c.customer_city ilike '%concord%' 
                                            or c.customer_city ilike '%georgetown%' 
                                            or c.customer_city ilike '%ashland%' )
                )  
                or
                ( (c.customer_state = 'CA') and (c.customer_city ilike '%oakland%' 
                                                or c.customer_city ilike '%pleasant hill%' )
                )
                or
                ( (c.customer_state = 'TX') and (c.customer_city ilike '%arlington%'
                                            or c.customer_city ilike '%brownsville%')
                )
            )
    ),
    
    affected_customer_locations as (
        /*
            - adds a customer's geo_location based on their city and state
            - I decided to apply the affected customer before this
              to avoid processing irrelevant data
        */

        select 
            c.*,
            us.geo_location
        from affected_customers as c
        left join vk_data.resources.us_cities as us
            on upper(trim(c.customer_state)) = upper(trim(us.state_abbr))
            and upper(trim(c.customer_city)) = upper(trim(us.city_name)) 
    ),

    chicago_geolocation as (
        /*
            create a single row table to retrieve Chicago geo_location
        */

        select
            geo_location
        from vk_data.resources.us_cities 
        where city_name = 'CHICAGO' and state_abbr = 'IL'
        limit 1
    ),

    gary_geolocation as (
        /*
            create a single row table to retrieve Gary geo_location
        */

        select
            geo_location
        from vk_data.resources.us_cities 
        where city_name = 'GARY' and state_abbr = 'IN'
        limit 1
    ),

    affected_customers_with_suppy_store_distance as (
        /*
            - calculates the distance between the customer and city
            - note: each cross join won't create additional rows since there is 
              only one result in each city-specific table
        */

        select
            c.*,
            (st_distance(c.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles,
            (st_distance(c.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
        from affected_customer_locations as c
        cross join chicago_geolocation as chicago
        cross join gary_geolocation as gary
    ),

    customer_preferences as (
        /* 
            - customer preferences can be joined last so we aren't passing 
              non-essential data around
        */
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
left join customer_preferences as cp
    on c.customer_id = cp.customer_id