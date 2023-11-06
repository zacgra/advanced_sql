/*
    I attempted to follow the Brookly Data Co style guide.  Please let me know
    if something is misaligned with the guide! :) 
    https://github.com/brooklyn-data/co/blob/main/sql_style_guide.md

    My approach in refactoring was to break each transformation or set of related
    transformations in to a CTE. (In Brooklyn Data Co they refer to this as a 
    logical unit of work.) In the final select, I join the customer geo/distance
    data with customer survey results.
*/

with     
    customers as (
        /*  Consolidates pre-query customer data */
        select
            cd.customer_id as id
            , cd.first_name || ' ' || cd.last_name as customer_name
            , ca.customer_city
            , ca.customer_state
        from vk_data.customers.customer_address as ca
        inner join vk_data.customers.customer_data cd on 
            ca.customer_id = cd.customer_id
    )

    , customers_affected as (
        /*  Filters out unaffected customers */
        select c.*
        from customers as c
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
    )
    
    , customers_affected_with_locations as (
        /* Adds to affected customers the the geo location of each customer */
        select
            c.*
            , us.geo_location
        from customers_affected as c
        left join vk_data.resources.us_cities as us on
            upper(trim(c.customer_state)) = upper(trim(us.state_abbr))
            and upper(trim(c.customer_city)) = upper(trim(us.city_name)) 
    )
    
    , chicago_geolocation as (
        /* Returns single row table with geo_location of Chicago */
        select geo_location
        from vk_data.resources.us_cities 
        where city_name = 'CHICAGO' and state_abbr = 'IL'
        limit 1
    )
    
    , gary_geolocation as (
        /* Returns single row table with geo_location of Gary */
        select geo_location
        from vk_data.resources.us_cities 
        where city_name = 'GARY' and state_abbr = 'IN'
        limit 1
    )
    
    , customers_affected_with_locations_and_distances as (
        /*  Adds to accumulating customers table a column with calculated 
            distance between the customer and supply city 
        */
        select
            c.*
            , (st_distance(c.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles
            , (st_distance(c.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
        from customers_affected_with_locations as c
        cross join chicago_geolocation as chicago
        cross join gary_geolocation as gary
    )
    
    , customer_preferences as (
        /*  Creates a table of the number of active survey results for each customer */ 
        select
            cs.customer_id
            , count(*) as food_pref_count
        from vk_data.customers.customer_survey as cs
        where is_active = true
        group by 1
    )
    
select
    c.customer_name
    , c.customer_city
    , c.customer_state
    , cp.food_pref_count
    , c.chicago_distance_miles
    , c.gary_distance_miles
from customers_affected_with_locations_and_distances as c
inner join customer_preferences as cp on c.id = cp.customer_id