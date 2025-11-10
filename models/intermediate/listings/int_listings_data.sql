{{
  config(
    materialized = 'incremental',
    unique_key = ['listing_id'],
    strategy = 'merge'
    )
}}

with base as (
    select
        listing_id,
        listing_name,
        host_id,
        listing_neighborhood,
        listing_property_type,
        CASE 
            WHEN REGEXP_SUBSTR(lower(listing_room_type), 'entire') is not null then 'Entire'
            WHEN REGEXP_SUBSTR(lower(listing_room_type), 'private|pvt') is not null then 'Private'
            ELSE 'Other' end AS listing_rooms_type,
        no_of_persons_accomodates,
        coalesce(trim(REGEXP_SUBSTR(bathroom_availability, '\\d+(\\.\\d+)?')),1)::float AS no_of_bathrooms,
        CASE
            WHEN REGEXP_SUBSTR(lower(bathroom_availability), 'shar(ed|ing)') is not null then 'Sharing'
            WHEN bathroom_availability is null then 'NA'
            ELSE 'Private' end AS listing_bathroom_type,
        no_of_bedrooms,
        no_of_beds,
    from {{ ref('stg_aws_s3__listings_data') }}
),

-- latest review stats from intermediate view
reviews_agg as (
    select listing_id, no_of_reviews, listing_first_review, listing_last_review, listing_avg_review_scores
    from {{ ref('int_aggregated_metrics_listings_reviews') }}
),

-- latest amenities per listing from snapshot
latest_amenities as (
    select listing_id, amenities_available_array
    from {{ ref('snap_stg_aws_s3__amenities_changelog') }}
    where dbt_valid_to is null
),

-- listing latest price from intermediate view
listing_latest_price as (
    select listing_id, listing_latest_price_usd
    from {{ ref('int_listing_latest_price') }}
)

select b.*, r.* exclude listing_id, a.* exclude listing_id, p.* exclude listing_id
from base b
left join reviews_agg r
using(listing_id)
left join latest_amenities a
using(listing_id)
left join listing_latest_price p
using(listing_id)
