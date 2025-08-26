 1)	For Production Quantities:
•	Azure Databricks Database
•	SQL Queries Used: 

1.	SELECT
      IPN_MV_LINE,
      MES_LOCATION_ID,
      SHIFT_ID, -- 0 = early shift, 1 = late shift, 2 = night shift
      WORKING_DAY_LOCAL_TS,
      COUNT_TARGET_OEE,
      COUNT_IO AS produced_parts,
      round((TIME_IO / TIME_TARGET_OEE_100), 2) AS OEE,
      year,
      month,
      day
FROM ps_xplatform_prod.mas.mfg_db_ipn_mv_agg_data_raw
WHERE PLANT_CODE = '0400' AND
      PRODUCT_AREA_CODE = 'KTK1'

2.	SELECT
IPN_MV_LINE,
MES_LOCATION_ID,
TYPE_NUMBER,
      SHIFT_ID,
      WORKING_DAY_LOCAL_TS,
      COUNT_TARGET_OEE,
      sum(COUNT_IO) as produced_parts,
      year,
      month,
      day
FROM ps_xplatform_prod.mas.mfg_db_ipn_mv_agg_type_data_raw
WHERE PLANT_CODE = '0400' AND
      PRODUCT_AREA_CODE = 'KTK1'
GROUP BY IPN_MV_LINE, MES_LOCATION_ID, TYPE_NUMBER, SHIFT_ID, COUNT_TARGET_OEE, WORKING_DAY_LOCAL_TS, year, month, day
3.	SELECT 
    a.IPN_MV_LINE,
    a.MES_LOCATION_ID,
    a.SHIFT_ID,  -- 0 = early shift, 1 = late shift, 2 = night shift
    a.WORKING_DAY_LOCAL_TS,
    a.COUNT_IO,
    round((a.TIME_IO / a.TIME_TARGET_OEE_100), 2) AS OEE,
    a.year,
    a.month,
    a.day,
    b.TYPE_NUMBER,  -- Part number information
    b.produced_parts
FROM 
    ps_xplatform_prod.mas.mfg_db_ipn_mv_agg_data_raw a
JOIN 
    (SELECT
        IPN_MV_LINE,
        MES_LOCATION_ID,
        TYPE_NUMBER,
        SHIFT_ID,
        WORKING_DAY_LOCAL_TS,
        sum(COUNT_IO) as produced_parts,
        year,
        month,
        day
    FROM 
        ps_xplatform_prod.mas.mfg_db_ipn_mv_agg_type_data_raw
    WHERE 
        PLANT_CODE = '0400' AND
        PRODUCT_AREA_CODE = 'KTK1'
    GROUP BY 
        IPN_MV_LINE, MES_LOCATION_ID, TYPE_NUMBER, SHIFT_ID, WORKING_DAY_LOCAL_TS, year, month, day) b
ON 
    a.IPN_MV_LINE = b.IPN_MV_LINE
    AND a.MES_LOCATION_ID = b.MES_LOCATION_ID
    AND a.SHIFT_ID = b.SHIFT_ID
    AND a.WORKING_DAY_LOCAL_TS = b.WORKING_DAY_LOCAL_TS
    AND a.year = b.year
    AND a.month = b.month
    AND a.day = b.day
WHERE 
    a.PLANT_CODE = '0400' 
    AND a.PRODUCT_AREA_CODE = 'KTK1'


2)	For Packaging Quantities:

Server Name: REDLake_ZeusP_Consumer_DALI.world
SQL Query: 
SELECT 
    MATNR,                 -- Material Number
    WERKS,                 -- Plant
    BUDAT_MKPF,            -- Posting Date
    ERFMG,                 -- Quantity
    BWART,                 -- Movement Type
    BNAME_MKPF_USNAM AS USERNAME  -- Username
FROM MARD_dali_bbm.V_MSEG_USR02_P81
WHERE BWART = '131'
  AND WERKS = '0400'

