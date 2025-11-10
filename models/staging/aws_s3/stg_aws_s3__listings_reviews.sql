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
      /*
      In the source data, we have two entries with review_id as null
      These get detected in when the dbt build is run and tests are run.
      In order to proceed with modeling, I added this filter.
      */
      where review_id is not null
  )
  select * from renamed
    