with calendar_prices as (
select listing_id, listing_in_point_price_usd listing_price_usd from {{ ref('stg_aws_s3__listings_reservation_calendar') }} 
qualify min(calendar_date) over(partition by listing_id) = calendar_date)  -- as per the schema provided, it should be min. But in my obervation, it will be max

, listing_prices as (
select listing_id, listing_price_usd
from {{ ref('stg_aws_s3__listings_data') }} 
)

-- Price is consistent in source listing table. 
select a.listing_id, a.listing_price_usd calender_price, b.listing_price_usd listing_price
from calendar_prices a
join listing_prices b
using(listing_id)
where a.listing_price_usd <> b.listing_price_usd
-- 60029, 78, 80

/* 
-- example
select distinct listing_in_point_price_usd
from  {{ ref('stg_aws_s3__listings_reservation_calendar') }}
where listing_id=60029;
*/