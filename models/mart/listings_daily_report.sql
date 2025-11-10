
with listing_calendar_data as (
  select * from {{ ref('int_listings_reservation_calendar') }}
),

listings as (
  select * from {{ ref('int_listings_data') }}
),

host as (
  select * from {{ ref('int_host_data') }}
)

select *
from listing_calendar_data c
left join listings l 
using (listing_id)
left join host h
using(listing_id, host_id)

