with 
    unique_sessions_ranked as (
        select
            *,
            row_number() over (partition by session_id order by event_timestamp) as event_position
        from vk_data.events.website_activity
    ),
    
    search_activity as (
        select
            session_id,
            event_id, 
            event_details,
            min(event_position) over (partition by event_id order by event_position) as first_search_event_pos
        from unique_sessions_ranked
        where event_details ilike '%view_recipe%'
    ),

    search_metrics as (
        select
            round(avg(first_search_event_pos), 1) as avg_searches_to_recipe_view
        from search_activity
    ),

    session_activity as (
        select
            session_id,
            min(event_timestamp) over (partition by session_id order by event_id) as first_session_event,
            max(event_timestamp) over (partition by session_id order by event_id) as last_session_event
        from vk_data.events.website_activity
    ),

    session_metrics as (
        select
            -- count(unique_sessions) as total_unique_sessions,
            round(avg(timestampdiff('second',first_session_event,last_session_event)), 1) as avg_session_duration
        from session_activity
    ),
    
    recipe_activity as (
        select
            parse_json(event_details) as json_payload,
            json_payload:"recipe_id"::text as recipe_id,
            count(recipe_id) as recipe_views
        from vk_data.events.website_activity
        where event_details ilike '%recipe%'
        group by event_details
    ),

    recipe_metrics as (
        select
            round(avg(first_search_event_pos), 1) as avg_searches_to_recipe_view 
        from search_activity
    )

select
    -- session_metrics.total_unique_sessions,
    session_metrics.avg_session_duration,
    search_metrics.avg_searches_to_recipe_view
    -- recipe_metrics.most_viewed_recipe
from session_metrics
join search_metrics
join recipe_metrics