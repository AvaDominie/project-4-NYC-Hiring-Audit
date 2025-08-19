{{ config(materialized='table') }}

WITH BASE AS (
    SELECT
        AGENCY_NAME,
        TITLE_DESCRIPTION AS JOB_TITLE,
        BASE_SALARY,
        PAY_BASIS,
        -- Convert hourly to annual (assume 2080 hours/year), else use base salary
        CASE
            WHEN UPPER(PAY_BASIS) = 'PER HOUR' THEN BASE_SALARY * 2080
            ELSE BASE_SALARY
        END AS ANNUAL_SALARY
    FROM {{ source('BRONZE', 'PAYROLL_DATA') }}
    WHERE BASE_SALARY IS NOT NULL
)

SELECT
    AGENCY_NAME,
    JOB_TITLE,
    ANNUAL_SALARY,
    PAY_BASIS
FROM BASE