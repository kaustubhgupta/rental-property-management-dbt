/*
Write a query to find the maximum duration one could stay in each of these
listings, based on the availability and what the owner allows.
*/

with 

-- listings date data which are available, new column for getting previous date using lag function
listing_date_data as (
select
listing_id, 
calendar_date, 
minimum_nights_to_book, 
maximum_nights_to_book, lag(calendar_date) over(partition by listing_id order by calendar_date asc) previous_calendar_day
from {{ ref('listings_daily_report') }}
where is_available )

-- creating a new flag to capture how many consective days can exists per listing
,flag_data as
  (SELECT
    listing_id,
    calendar_date,
    CASE
      WHEN previous_calendar_day IS NULL THEN 1                   
      WHEN datediff('day', previous_calendar_day, calendar_date) = 1 THEN 0  
      ELSE 1                                          
    END AS reverse_continue
  FROM listing_date_data)

-- running total for num_days
, num_days_data as (
  SELECT
    listing_id,
    calendar_date,
    SUM(reverse_continue) OVER (PARTITION BY listing_id ORDER BY calendar_date ROWS UNBOUNDED PRECEDING) + 1 AS num_days
  FROM flag_data
  )

-- each gap counted as days available, min max dates give the boundary of this availability
, listing_available_stay_durations as (
SELECT
  listing_id,
  MIN(calendar_date) AS start_date,
  MAX(calendar_date) AS end_date,
  COUNT(*)           AS num_days_available
FROM num_days_data
GROUP BY listing_id, num_days
ORDER BY listing_id, start_date)

/*
final selection while checking if the num of days available are within listing min man nights
also, used window function filter qualify to filter on listings where max days of dataset matches
*/
select a.*, b.minimum_nights_to_book, b.maximum_nights_to_book
from listing_available_stay_durations a
left join (select distinct listing_id, minimum_nights_to_book, maximum_nights_to_book from listing_date_data) b
using(listing_id)
where num_days_available between b.minimum_nights_to_book and b.maximum_nights_to_book
qualify max(num_days_available) over() = num_days_available