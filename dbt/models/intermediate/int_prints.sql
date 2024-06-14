{{
  config(
    materialized="table",
    schema="intermediate"
  )
}}

WITH RN AS (
  SELECT
      id
      ,day
      ,position
      ,value_prop
      ,user_id
      ,ROW_NUMBER() OVER (PARTITION BY day, position, value_prop, user_id ORDER BY id DESC) AS rn
  FROM {{ source('value_props', 'prints')}}
)

SELECT
  day
  ,position
  ,value_prop
  ,user_id
FROM RN
WHERE rn = 1