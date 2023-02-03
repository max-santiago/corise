/* I use inner joins in the first CTE as a method to filter for customer elligibility. */
/* I use the row_number() function and the QUALIFY clause to find the best supplier for
   each customer. */

with 
    us_cities as (
        select
            city_name
            , state_abbr
            , geo_location
        from resources.us_cities
        qualify row_number() over (partition by city_name, state_abbr order by county_name) = 1
    )

    , customers as (
        select
            customer_data.*
            , us_cities.geo_location
        from customers.customer_data
        inner join customers.customer_address
            on customer_data.customer_id = customer_address.customer_id
        inner join us_cities
            on upper(trim(customer_address.customer_city)) = us_cities.city_name
            and customer_address.customer_state = us_cities.state_abbr
    )

    , suppliers as (
        select
            supplier_info.*
            , us_cities.geo_location
        from suppliers.supplier_info
        inner join us_cities
            on upper(trim(supplier_info.supplier_city)) = us_cities.city_name
            and supplier_info.supplier_state = us_cities.state_abbr
    )

select
    customers.customer_id
    , customers.first_name
    , customers.last_name
    , customers.email
    , suppliers.supplier_id
    , suppliers.supplier_name
    , st_distance(customers.geo_location, suppliers.geo_location) / 1609 as distance_in_miles
from customers
cross join suppliers
qualify row_number() over (partition by customers.customer_id order by distance_in_miles asc) = 1
order by
    customers.last_name
    , customers.first_name
