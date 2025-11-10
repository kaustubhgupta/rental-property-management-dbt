with source as (
        select * from {{ source('aws_s3', 'listings_reviews') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ID") }} AS review_id,
        {{ adapter.quote("LISTING_ID") }} AS listing_id,
        {{ adapter.quote("REVIEW_SCORE") }} AS listing_review_score,
        {{ adapter.quote("REVIEW_DATE") }}::date AS listing_review_date

      from source
      where review_id is not null
  )
  select * from renamed
    