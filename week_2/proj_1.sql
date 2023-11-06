/*
    I attempted to follow the Brookly Data Co style guide.  Please let me know
    if something is misaligned with the guide! :) 
    https://github.com/brooklyn-data/co/blob/main/sql_style_guide.md

    My approach in refactoring was to break each transformation or set of related
    transformations in to a CTE. I eventually clump the data into a CTE with the 
    affected customers with geolocation/distances, then another CTE with survey 
    results.  In the final select, I join these two sources.
*/

with     
    chicago_geolocation as (
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

    , customer_preferences as (
        /*  Creates table of number of active survey results for each customer */ 
        select
            customer_survey.customer_id
            , count(*) as food_pref_count
        from vk_data.customers.customer_survey
        where is_active = true
        group by 1
    )

    , affected_locations (id, city, state) as (
        /*  Creates CTE to more realistically replicate real world scenario */
        select
            *
        from values
                (1, 'concord',        'ky')
                ,(2, 'georgetown',    'ky')
                ,(3, 'ashland',       'ky')
                ,(4, 'oakland',       'ca')
                ,(5, 'pleasant hill', 'ca')
                ,(6, 'arlington',    'tx')
                ,(7, 'brownsville',   'tx')
    )

    , customers_affected as (
        /* Filters customers by affected locations */
        select
            customer_data.customer_id as id
            , customer_data.first_name || ' ' || customer_data.last_name as customer_name
            , ca.customer_city
            , ca.customer_state
        from vk_data.customers.customer_address as ca
        inner join vk_data.customers.customer_data on
            ca.customer_id = customer_data.customer_id
        inner join affected_locations on
            lower(trim(ca.customer_city)) = affected_locations.city 
            and lower(trim(ca.customer_state)) = affected_locations.state
    )

    , customers_affected_with_geolocations as (
        /* Adds geolocation to affected customers */
        select
            customers_affected.*
            , us.geo_location
        from customers_affected
        left join vk_data.resources.us_cities as us on
            upper(trim(customers_affected.customer_state)) = upper(trim(us.state_abbr))
            and upper(trim(customers_affected.customer_city)) = upper(trim(us.city_name)) 
    )
    
    , customers_affected_with_geolocations_and_distances as (
        /* Adds distances between customer and cities to affected customers */
        select
            customers_awg.*
            , (st_distance(customers_awg.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles
            , (st_distance(customers_awg.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
        from customers_affected_with_geolocations as customers_awg
        cross join chicago_geolocation as chicago
        cross join gary_geolocation as gary
    )

select
    customers_affected.customer_name
    , customers_affected.customer_city
    , customers_affected.customer_state
    , customer_preferences.food_pref_count
    , customers_affected.chicago_distance_miles
    , customers_affected.gary_distance_miles
from customers_affected_with_geolocations_and_distances as customers_affected
inner join customer_preferences on customers_affected.id = customer_preferences.customer_id