
-- This script cleans and aggregates the treated municipal wastewater data (17 csv files)
-- into two analysis-ready tables 
-- Study period: 2008 – 2024 (Ontario, Canada)

CREATE TABLE YOUR_PROJECT.YOUR_DATASET.MWTE_Ontario_Canada_2008_2024 AS

-- Step 1: Combine annual datasets (2008–2024) into a single table

WITH table_1 
  AS (
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2008
  UNION ALL
  SELECT
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2009
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2010
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2011
  UNION ALL
  SELECT
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2012
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2013
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2014
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2015
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2016
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2017
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2018
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2019
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2020
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2021
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2022
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2023
  UNION ALL
  SELECT 
    *
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWD2024
),

-- Step 2: Remove duplicates and data standardization 

table_2 AS (
  SELECT DISTINCT
    CASE
      WHEN ministry_region = 'MOECC CENTRAL REGION' then 'Central'
      WHEN ministry_region = 'MOECC EASTERN REGION' then 'Eastern'
      WHEN ministry_region = 'MOECC NORTHERN REGION' then 'Northern'
      WHEN ministry_region = 'MOECC SOUTHWESTERN REGION' then 'South Western'
      WHEN ministry_region = 'MOECC WEST CENTRAL REGION' then 'West Central'
      END AS ministry_region,
    treatment_type,
    works_name,
    municipality,
    watershed,
    month as year_month,
    CASE
      WHEN parameter_name ='PHOSPHORUS-UNFILTERED TOTAL' THEN 'Unfiltered TP'
      WHEN parameter_name ='RESIDUE-PARTICULATE' THEN 'Residue Particulate'
      WHEN parameter_name ='SEWAGE FLOW-MONTHLY TOTAL' THEN 'Sewage Flow'
      END AS parameter_name, 
      reported_value AS value
  FROM 
    table_1
  WHERE
    parameter_name IN ('PHOSPHORUS-UNFILTERED TOTAL', 'RESIDUE-PARTICULATE', 'SEWAGE FLOW-MONTHLY TOTAL')
),

-- Step 3: 
-- Safely convert string date to DATE format, and values to FLOAT64,
-- Extract month and year components,
-- Filter out negative records 

table_3 AS (
  SELECT
    ministry_region,
    treatment_type,
    works_name,
    municipality,
    watershed,
    SAFE.PARSE_DATE('%y-%b', year_month) AS date,
    EXTRACT(YEAR FROM SAFE.PARSE_DATE('%y-%b', year_month)) AS year,
    EXTRACT(MONTH FROM SAFE.PARSE_DATE('%y-%b', year_month)) AS month,
    parameter_name,
    SAFE_CAST(value AS FLOAT64) as value
  FROM 
    table_2
  WHERE
    SAFE_CAST(value AS FLOAT64) >= 0
),

-- Step 4:
-- Extract monthly average Unfiltered Total Phosphorus (TP) values,
-- Filter only 'Unfiltered TP' records and round for reporting consistency

table_4 AS (
  SELECT
    ministry_region,
    treatment_type,
    works_name,
    municipality,
    watershed,
    date,
    year,
    month,
    parameter_name AS unfiltered_TP,
    ROUND(value, 1) AS monthly_avg_unfiltered_TP_mg_per_L
  FROM 
    table_3
  WHERE
    parameter_name = 'Unfiltered TP'
),

-- Step 5:
-- Extract monthly average particulate residue values,
-- Filter only 'Residue Particulate' records and round for reporting consistency

table_5 AS (
  SELECT
    ministry_region,
    treatment_type,
    works_name,
    municipality,
    watershed,
    date,
    year,
    month,
    parameter_name AS residue_particulate,
    ROUND(value, 1) AS monthly_avg_residue_particulate_mg_per_L
  FROM 
    table_3
  WHERE
    parameter_name = 'Residue Particulate'
),

-- Step 6:
-- Extract monthly total flow values
-- Filter only 'Sewage Flow' records and round for reporting consistency

table_6 AS (
  SELECT
    ministry_region,
    treatment_type,
    works_name,
    municipality,
    watershed,
    date,
    year,
    month,
    parameter_name AS flow,
    ROUND(value, 3) AS monthly_tot_flow_thousand_m3
  FROM 
    table_3
  WHERE
    parameter_name = 'Sewage Flow'
),

-- Step 7:
-- Join tables 4, 5, and 6
-- Calculate pollutant loads (unfiltered TP and particulate residue)

table_7 AS (
  SELECT
    table_4.ministry_region,
    table_4.treatment_type,
    table_4.works_name,
    table_4.municipality,
    table_4.watershed,
    table_4.date,
    table_4.year,
    table_4.month,
    table_4.monthly_avg_unfiltered_TP_mg_per_L,
    table_5.monthly_avg_residue_particulate_mg_per_L,
    table_6.monthly_tot_flow_thousand_m3,
    ROUND((table_4.monthly_avg_unfiltered_TP_mg_per_L*table_6.monthly_tot_flow_thousand_m3), 1) AS  monthly_avg_unfiltered_TP_in_kg,
    ROUND((table_5.monthly_avg_residue_particulate_mg_per_L*table_6.monthly_tot_flow_thousand_m3), 1) AS monthly_avg_residue_particulate_in_kg
  FROM 
    table_4
  INNER JOIN
    table_5
  ON
    table_4.ministry_region =table_5.ministry_region
  AND
    table_4.treatment_type = table_5.treatment_type
  AND
    table_4.works_name = table_5.works_name
  AND
    table_4.municipality = table_5.municipality
  AND
    table_4.watershed = table_5.watershed
  AND
    table_4.date = table_5.date
  INNER JOIN
    table_6
  ON
    table_4.ministry_region =table_6.ministry_region
  AND
    table_4.treatment_type = table_6.treatment_type
  AND
    table_4.works_name = table_6.works_name
  AND
    table_4.municipality = table_6.municipality
  AND
    table_4.watershed = table_6.watershed
  AND
    table_4.date = table_6.date
)

-- Step 8: Filter out NULL records 

SELECT
  *
FROM
  table_7
WHERE 
  monthly_tot_flow_thousand_m3 IS NOT NULL;

CREATE TABLE YOUR_PROJECT.YOUR_DATASET.MWTE_QualifiedPlant_Ontario_Canada_2008_2024 AS

-- Step 9:
-- Ensure data completeness by filtering to plants with:
-- 1) Full monthly reporting (12 months/year)
-- 2) At least 10 years of reporting history

WITH table_8 AS (
  SELECT
    ministry_region,
    works_name,
    date,
    year,
    month,
    monthly_avg_unfiltered_TP_mg_per_L,
    monthly_avg_residue_particulate_mg_per_L,
    monthly_tot_flow_thousand_m3,
    monthly_avg_unfiltered_TP_in_kg,
    monthly_avg_residue_particulate_in_kg,
    COUNT(DISTINCT month) OVER (PARTITION BY works_name, year) AS months_per_year,
    COUNT(DISTINCT year)  OVER (PARTITION BY works_name) AS years_per_plant
  FROM 
    YOUR_PROJECT.YOUR_DATASET.MWTE_Ontario_Canada_2008_2024
)
SELECT 
  *
FROM 
  table_8
WHERE 
  months_per_year = 12
  AND years_per_plant >= 10;


