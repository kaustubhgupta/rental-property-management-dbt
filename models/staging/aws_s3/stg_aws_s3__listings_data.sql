with source as (
        select * from {{ source('aws_s3', 'listings_data') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ID") }} AS listing_id,
        {{ adapter.quote("NAME") }} AS listing_name,
        {{ adapter.quote("HOST_ID") }} AS host_id,
        {{ adapter.quote("HOST_NAME") }} AS host_name,
        {{ adapter.quote("HOST_SINCE") }}::date AS host_since_date,
        {{ adapter.quote("HOST_LOCATION") }} AS host_location,
        {{ adapter.quote("HOST_VERIFICATIONS") }}::array AS host_verification_methods_array,  -- renaming as per data type
        {{ adapter.quote("NEIGHBORHOOD") }} AS listing_neighborhood,
        {{ adapter.quote("PROPERTY_TYPE") }} AS listing_property_type,
        {{ adapter.quote("ROOM_TYPE") }} AS listing_room_type,
        {{ adapter.quote("ACCOMMODATES") }} AS no_of_persons_accomodates,
        {{ adapter.quote("BATHROOMS_TEXT") }} AS bathroom_availability,
        COALESCE({{ adapter.quote("BEDROOMS") }},1) AS no_of_bedrooms,  -- default value to 1 for nulls
        COALESCE({{ adapter.quote("BEDS") }},1) AS no_of_beds,  -- default value to 1 for nulls
        -- all the below columns are to be refreshed based on latest data in intermediate tables/views
        {{ adapter.quote("AMENITIES") }}::array AS amenities_available_array,
        regexp_replace({{ adapter.quote("PRICE") }}, '[$,]', '')::float AS listing_price_usd,  -- cleaning price column to remove currency sign
        {{ adapter.quote("NUMBER_OF_REVIEWS") }} AS no_of_reviews,
        {{ adapter.quote("FIRST_REVIEW") }}::date AS listing_first_review,
        {{ adapter.quote("LAST_REVIEW") }}::date AS listing_last_review,
        {{ adapter.quote("REVIEW_SCORES_RATING") }}::float listing_avg_review_scores

      from source
  )
  select * from renamed
    