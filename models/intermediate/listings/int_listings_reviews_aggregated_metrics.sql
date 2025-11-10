{{
  config(
    materialized = 'view',
    )
}}

/*
  Aggregated meteics view for listings reviews.
  This view will return latest metrics for reviews shared for listings.
  count of reviews, listing first and latest review date and average review score of listings
 */

select
    listing_id,
    count(distinct review_id)::integer as no_of_reviews,
    min(listing_review_date)::date as listing_first_review,
    max(listing_review_date)::date as listing_last_review,
    round(avg(listing_review_score),2)::float as listing_avg_review_scores
from {{ ref('int_listings_reviews') }}
group by listing_id