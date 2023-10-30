with unique_cities
    as (select distinct
            uc.city_name,
            uc.state_abbr,
            uc.geo_location
        from vk_data.resources.us_cities uc
        ),

    eligible_customers
        as (
            select
                ca.customer_id
            from unique_cities as uc
            join vk_data.customers.customer_address as ca
                on lower(trim(uc.city_name)) = lower(trim(ca.customer_city)) 
                and lower(trim(uc.state_abbr)) = lower(trim(ca.customer_state))
        ),
        
    customer_preferences
        as ( 
            select * from (
                select
                    ec.customer_id,
                    rt.tag_property,
                    row_number() over (partition by ec.customer_id order by rt.tag_property) as rn
                from eligible_customers ec
                join vk_data.customers.customer_survey cs
                    on ec.customer_id = cs.customer_id
                join vk_data.resources.recipe_tags rt
                    on cs.tag_id = rt.tag_id
                order by ec.customer_id, rt.tag_property
                )
            where rn <= 3
        ),
    customer_recipe_suggestion
        as ( 
            select 
                *
            from (
                select
                    *
                from customer_preferences
                where rn = 1
            )
            -- need to figure out how to return rows from chef.recipe
            -- that contain pref_1, then limit 1
        )
select
    cp.customer_id,
    cd.email,
    cd.first_name,
    max(case when rn = 1 then tag_property end) pref_1,
    max(case when rn = 2 then tag_property end) pref_2,
    max(case when rn = 3 then tag_property end) pref_3
from customer_preferences cp
join vk_data.customers.customer_data cd
    on cp.customer_id = cd.customer_id
group by cp.customer_id, cd.email, cd.first_name