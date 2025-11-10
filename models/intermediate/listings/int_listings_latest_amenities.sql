{{
  config(
    materialized = 'view',
    )
}}

/* 
  Listing latest amenities view.
  This view will return latest amenities of listing from snapshot
 */

select listing_id, amenities_available_array
from {{ ref('snap_stg_aws_s3__amenities_changelog') }}
where dbt_valid_to is null