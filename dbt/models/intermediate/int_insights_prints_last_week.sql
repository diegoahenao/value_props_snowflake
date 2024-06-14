{{
  config(
    materialized="table",
    schema="intermediate"
  )
}}

WITH LAST_WEEK_PRINTS AS (
    SELECT 
        DAY 
        ,POSITION 
        ,VALUE_PROP 
        ,USER_ID 
    FROM 
        {{ ref('int_prints') }}
    WHERE 
        DAY BETWEEN (SELECT MAX(DAY) FROM {{ ref('int_prints') }}) - INTERVAL '7 DAYS' AND (SELECT MAX(DAY) FROM {{ ref('int_prints') }})
    ),
    LAST_WEEK_PRINTS_CLICKS AS (
    SELECT 
        LWP.DAY 
        ,LWP.POSITION 
        ,LWP.VALUE_PROP 
        ,LWP.USER_ID
        ,CASE
            WHEN IT.USER_ID IS NOT NULL THEN 1
            ELSE 0
        END AS IF_CLICK
    FROM 
        LAST_WEEK_PRINTS LWP
    LEFT JOIN 
        {{ ref('int_taps') }} IT
    ON LWP.DAY = IT.DAY
    AND LWP.POSITION = IT.POSITION
    AND LWP.VALUE_PROP = IT.VALUE_PROP
    AND LWP.USER_ID = IT.USER_ID
    ),
    VIEWS_THREE_WEEKS_BF_PRINT AS (
        SELECT
            LWPC.DAY 
            ,LWPC.POSITION
            ,LWPC.VALUE_PROP 
            ,LWPC.USER_ID
            ,LWPC.IF_CLICK
            ,COUNT(IP.VALUE_PROP) AS VIEW_COUNT_VALUE_PROP
        FROM
           LAST_WEEK_PRINTS_CLICKS LWPC
        LEFT JOIN
            {{ ref('int_prints') }} IP
        ON LWPC.USER_ID = IP.USER_ID
        AND LWPC.VALUE_PROP = IP.VALUE_PROP
        AND IP.DAY BETWEEN LWPC.DAY - INTERVAL '21 DAYS' AND LWPC.DAY - INTERVAL '1 DAYS'
        GROUP BY 1, 2, 3, 4, 5
    ),
    CLICKS_THREE_WEEKS_BF_PRINT AS (
        SELECT
            VWBFP.DAY
            ,VWBFP.POSITION
            ,VWBFP.VALUE_PROP
            ,VWBFP.USER_ID
            ,VWBFP.IF_CLICK
            ,VWBFP.VIEW_COUNT_VALUE_PROP
            ,COUNT(IT.VALUE_PROP) AS CLICKS_COUNT_VALUE_PROP
        FROM
            VIEWS_THREE_WEEKS_BF_PRINT VWBFP
        LEFT JOIN
            {{ ref('int_taps') }} IT
        ON VWBFP.USER_ID = IT.USER_ID
        AND VWBFP.VALUE_PROP = IT.VALUE_PROP
        AND IT.DAY BETWEEN VWBFP.DAY - INTERVAL '21 DAYS' AND VWBFP.DAY - INTERVAL '1 DAYS'
        GROUP BY 1, 2, 3, 4, 5, 6
    ),
    PAYMENT_THREE_WEEKS_BF_PRINT AS (
        SELECT
            CBFP.DAY
            ,CBFP.POSITION
            ,CBFP.VALUE_PROP
            ,CBFP.USER_ID
            ,CBFP.IF_CLICK
            ,CBFP.VIEW_COUNT_VALUE_PROP
            ,CBFP.CLICKS_COUNT_VALUE_PROP
            ,COUNT(IPY.VALUE_PROP) AS PAYMENT_COUNT_VALUE_PROP
            ,SUM(IPY.TOTAL) AS TOTAL_VALUE_PROP
        FROM
            CLICKS_THREE_WEEKS_BF_PRINT CBFP
        LEFT JOIN
            {{ ref('int_pays') }} IPY
        ON CBFP.USER_ID = IPY.USER_ID
        AND CBFP.VALUE_PROP = IPY.VALUE_PROP
        AND IPY.PAY_DATE BETWEEN CBFP.DAY - INTERVAL '21 DAYS' AND CBFP.DAY - INTERVAL '1 DAYS'
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )
SELECT
    PBFP.DAY
    ,PBFP.POSITION
    ,PBFP.VALUE_PROP
    ,PBFP.USER_ID
    ,PBFP.IF_CLICK
    ,PBFP.VIEW_COUNT_VALUE_PROP
    ,PBFP.CLICKS_COUNT_VALUE_PROP
    ,PBFP.PAYMENT_COUNT_VALUE_PROP
    ,PBFP.TOTAL_VALUE_PROP
FROM
    PAYMENT_THREE_WEEKS_BF_PRINT PBFP