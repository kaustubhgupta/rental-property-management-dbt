
/* 
  The final mart table where everything joins. This table is created at listing-calendar day grain
  It allows to perform period over period analysis across revenue and occupancy
  All relevant intermediate tables are joined here and made as much denormalized it can go
 */

with listing_calendar_data as (
  select * from {{ ref('int_listings_reservation_calendar') }}
),

listings as (
  select * from {{ ref('int_listings_data') }}
),

host as (
  select * from {{ ref('int_host_data') }}
)

/*
Above had everything selected but in final selection, added all columns with source tables
This will allow us to control what goes into final tables and avoids any data leakage 
*/
select
    c.listing_id,
    l.host_id,
    h.host_location,
    h.host_name,
    h.host_since_date,
    h.host_verification_methods_array,
    c.calendar_date,    
    c.is_available,
    c.reservation_id,
    c.minimum_nights_to_book,
    c.maximum_nights_to_book,    
    c.listing_in_point_price_usd,
    l.listing_latest_price_usd,
    l.listing_name,
    l.listing_neighborhood,
    l.listing_property_type,
    l.amenities_available_array,
    l.listing_rooms_type,
    l.no_of_bathrooms,
    l.listing_bathroom_type,
    l.no_of_bedrooms,
    l.no_of_beds,
    l.no_of_persons_accomodates,
    l.no_of_reviews,
    l.listing_first_review,
    l.listing_last_review,
    l.listing_avg_review_scores
from listing_calendar_data c
left join listings l 
using (listing_id)
left join host h
using(listing_id, host_id)

