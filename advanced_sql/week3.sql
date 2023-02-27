ALTER SESSION SET USE_CACHED_RESULT = FALSE;

with
	events as (
        select
        	event_id
            , any_value(session_id) as session_id
            , any_value(user_id) as user_id
            , any_value(event_timestamp) as event_timestamp
            , parse_json(any_value(event_details)):event::text as event_type
            , parse_json(any_value(event_details)):recipe_id::text as recipe_id

        from vk_data.events.website_activity
        group by 1
	)

-- pre-compute session-level aggregates before joining back to events
, sessions as (
    select
    	session_id
        , min(iff(recipe_id is not null, event_timestamp, null)) as first_recipe_at
        , datediff('seconds', min(event_timestamp), max(event_timestamp)) as session_length
        , avg(session_length) over () as average_session_length
        , count(session_id) over () as session_count
        , count(first_recipe_at) over () as recipe_sessions

    from events
    group by 1
)

-- use any_value() to minimize the impact of the aggregation step
select
	any_value(sessions.session_count) as session_count
    , any_value(sessions.average_session_length)::numeric(10,2) as average_session_length
    , (count(case 
    			when events.event_timestamp <= sessions.first_recipe_at
                	and events.event_type = 'search'
                then 1
            end) / any_value(sessions.recipe_sessions)
            )::numeric(10,2) as avg_searches_before_recipe
	, mode(events.recipe_id) as most_common_recipe
    
from events
left join sessions using (session_id)    
