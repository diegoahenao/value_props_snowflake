{{
  config(
    materialized="table",
    schema="mart"
  )
}}

SELECT
    DAY
    ,POSITION
    ,VALUE_PROP
    ,USER_ID
    ,IF_CLICK
    ,VIEW_COUNT_VALUE_PROP
    ,CLICKS_COUNT_VALUE_PROP
    ,PAYMENT_COUNT_VALUE_PROP
    ,TOTAL_VALUE_PROP
FROM
    {{ ref('int_insights_prints_last_week')}}