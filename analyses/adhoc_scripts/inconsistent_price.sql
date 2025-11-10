with calendar_prices as (
select listing_id, listing_in_point_price_usd listing_price_usd from {{ ref('stg_aws_s3__listings_reservation_calendar') }} 
qualify min(calendar_date) over(partition by listing_id) = calendar_date)

, listing_prices as (
select listing_id, listing_price_usd
from {{ ref('stg_aws_s3__listings_data') }} 
)

select a.listing_id, a.listing_price_usd calender_price, b.listing_price_usd listing_price
from calendar_prices a
join listing_prices b
using(listing_id)
where a.listing_price_usd <> b.listing_price_usd

{# select distinct listing_price_usd
from hubspot_assessment.stagging.stg_aws_s3__listings_reservation_calendar
where listing_id=60029; #}
-- 78, 80