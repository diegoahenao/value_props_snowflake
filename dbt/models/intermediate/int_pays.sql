{{
  config(
    materialized="table",
    schema="intermediate"
  )
}}

WITH RN AS (
  SELECT
      id
      ,pay_date
      ,total
      ,value_prop
      ,user_id
      ,ROW_NUMBER() OVER (PARTITION BY pay_date, total, value_prop, user_id ORDER BY id DESC) AS rn
  FROM {{ source('value_props', 'pays')}}
)

SELECT
  pay_date
  ,total
  ,value_prop
  ,user_id
FROM RN
WHERE rn = 1