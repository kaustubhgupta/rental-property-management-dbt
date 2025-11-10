{% snapshot snap_stg_aws_s3__amenities_changelog %}

{{
  config(
    target_schema='snapshots',
    unique_key='listing_id',
    strategy='timestamp',
    updated_at='change_at_date'
  )
}}

/*
  Amenities changelog has a consistent change timestamp and therfore has be moulded into SCD2 type implementation
  using timestamp strategy.
  To ingest this data properly, I used one adhoc script to fetch the first time changes
  from source file. Then snapshot was created for first time.
  After that I provided the full source so that old changesets are expired and new entires are created
 */

select
    listing_id,
    change_at_date,
    amenities_available_array
from {{ ref('stg_aws_s3__amenities_changelog') }}

{% endsnapshot %}
