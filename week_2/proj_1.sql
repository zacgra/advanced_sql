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

    , customers_affected as (
        /*  consolidates customer data and filters out unaffected customers
        */
        select
            customer_data.customer_id as id
            , customer_data.first_name || ' ' || customer_data.last_name as customer_name
            , ca.customer_city
            , ca.customer_state
        from vk_data.customers.customer_address as ca
        inner join vk_data.customers.customer_data on
            ca.customer_id = customer_data.customer_id
        where ((ca.customer_state = 'KY') and (ca.customer_city ilike '%concord%' 
                                                or ca.customer_city ilike '%georgetown%'
                                                or ca.customer_city ilike '%ashland%'))
            or ((ca.customer_state = 'CA') and (ca.customer_city ilike '%oakland%'
                                                or ca.customer_city ilike '%pleasant hill%'))
            or ((ca.customer_state = 'TX') and (ca.customer_city ilike '%arlington%'
                                                or ca.customer_city ilike '%brownsville%'))
    )

    , customers_affected_with_locations as (
        /* Adds to affected customers the the geo location of each customer */
        select
            customers_affected.*
            , us.geo_location
        from customers_affected
        left join vk_data.resources.us_cities as us on
            upper(trim(customers_affected.customer_state)) = upper(trim(us.state_abbr))
            and upper(trim(customers_affected.customer_city)) = upper(trim(us.city_name)) 
    )
    
    , customers_affected_with_locations_and_distances as (
        /*  Adds to accumulating customers table a column with calculated 
            distance between the customer and supply city
        */
        select
            customers_awl.*
            , (st_distance(customers_awl.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles
            , (st_distance(customers_awl.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
        from customers_affected_with_locations as customers_awl
        cross join chicago_geolocation as chicago
        cross join gary_geolocation as gary
    )
    
    , customer_preferences as (
        /*  Creates a table of the number of active survey results for each customer */ 
        select
            customer_survey.customer_id
            , count(*) as food_pref_count
        from vk_data.customers.customer_survey
        where is_active = true
        group by 1
    )
    
select
    customers_affected.customer_name
    , customers_affected.customer_city
    , customers_affected.customer_state
    , customer_preferences.food_pref_count
    , customers_affected.chicago_distance_miles
    , customers_affected.gary_distance_miles
from customers_affected_with_locations_and_distances as customers_affected
inner join customer_preferences on customers_affected.id = customer_preferences.customer_id