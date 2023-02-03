/* The gist of my approach consists of fetching customer preferences in long format, then pivoting them. */
/* The filtering of eligible customers is taken care of by virtue of the inner joins in the 2nd CTE. */
/* I also explode the recipes table to have one row per recipe-tag combo, and facilitate later joins. */

with 
    cities as (
        select
            city_name
            , state_abbr
        from resources.us_cities
        qualify row_number() over (partition by city_name, state_abbr order by city_name) = 1
    )

    , customers as (
        select
            customer_data.customer_id
            , customer_data.email
            , customer_data.first_name
        from customers.customer_data 
        inner join customers.customer_address 
            on customer_data.customer_id = customer_address.customer_id
        inner join cities
            on upper(trim(customer_address.customer_city)) = cities.city_name
            and customer_address.customer_state = cities.state_abbr
    )
    
    , preferences_long as (
        select
            customer_survey.customer_id
            , lower(trim(recipe_tags.tag_property)) as food_preference
            , dense_rank() over (partition by customer_survey.customer_id order by food_preference) as rank
        from customers.customer_survey
        left join resources.recipe_tags on customer_survey.tag_id = recipe_tags.tag_id
    )
    
    , preferences_wide as (
        select *
        from preferences_long
            pivot(max(food_preference) for rank in ('1', '2', '3'))
            as p (customer_id, food_pref_1, food_pref_2, food_pref_3)
    )

    , recipes as (
        select
            lower(trim(tags.value::text)) as recipe_tag
            , any_value(recipe.recipe_name) as suggested_recipe
        from chefs.recipe,
        lateral flatten(input => recipe.tag_list) as tags
        group by 1
    )

select
    customers.*
    , preferences_wide.food_pref_1
    , preferences_wide.food_pref_2
    , preferences_wide.food_pref_3
    , recipes.suggested_recipe
from customers
inner join preferences_wide on customers.customer_id = preferences_wide.customer_id
inner join recipes on preferences_wide.food_pref_1 = recipes.recipe_tag
