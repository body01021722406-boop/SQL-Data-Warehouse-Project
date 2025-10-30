PRINT('============================================================================')
PRINT('Bronze.crm_cust_info')
PRINT('============================================================================')
PRINT '>> Truncating Table: silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
-- Clean NULLs or Duplicates in Primary Key
SELECT * 
FROM(
	SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	) AS t
WHERE flag_last != 1



--Clean Unwanted Spaces
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date 
FROM (
	SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	) AS t
WHERE flag_last = 1



-- Claen Data Standardization & Consistency
SELECT
	cst_id,
	cst_key,
	Trim(cst_firstname) AS cst_firstname,
	Trim(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 when UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 else 'n/a'
	END cst_marital_status,	
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 when UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 else 'n/a'
	END cst_gndr,
	cst_create_date 
FROM (
	SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	) AS t
WHERE flag_last = 1

--SELECT * FROM silver.crm_cust_info


PRINT('============================================================================')
PRINT('Bronze.crm_prd_info')
PRINT('============================================================================')
PRINT '>> Truncating Table: silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;

-- Clean NULLs or Duplicates in Primary Key
SELECT 
	prd_id,
	COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


--لتعديل ال prd_key و يكون شكله زي ال PK اللي عايز اربطه مع جدول تاني 
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
  FROM bronze.crm_prd_info
  WHERE SUBSTRING(prd_key, 7, LEN(prd_key))  IN (
  SELECT sls_prd_key FROM bronze.crm_sales_details
  --SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
  )

--Final Result
  SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
  FROM bronze.crm_prd_info




-- Check for NULLs or Negative Values in Cost
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


--Clean Data Standardization & Consistency
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' 
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' 
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 Else 'n/a'
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info



-- Check for Invalid Date Orders (Start Date > End Date)
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' 
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' 
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	-- لحذف توقيت الساعة من داخل التاريخ لأنها كلها أصفار
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	-- هنا بنبدل التاريخ النهائي بالبدائي على حسب الترتيب
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt,
	GETDATE() AS dwh_create_date
FROM bronze.crm_prd_info;


--SELECT * FROM silver.crm_prd_info;



PRINT('============================================================================')
PRINT('Bronze.crm_sales_details')
PRINT('============================================================================')
PRINT '>> Truncating Table: silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;


--- Check for Invalid Dates
-- TO EXTRACT THE MISSING VALUSE AND MISTAKES
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112) IS NULL;


--2.FOR CONVERT THE DATE 
 SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112) IS NOT NULL 
			THEN TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112)
		ELSE NULL 
	END AS sls_order_dt,
	CASE 
		WHEN TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112) IS NOT NULL 
			THEN TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112)
		ELSE NULL 
	END AS sls_ship_dt,
	CASE 
		WHEN TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112) IS NOT NULL 
			THEN TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112)
		ELSE NULL 
	END AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details;



-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
SELECT 
	* 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;



-- Check Data Consistency: Sales = Quantity * Price
SELECT  
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,

	 CASE 
		WHEN TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112) IS NOT NULL 
			THEN TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112)
		ELSE NULL 
	END AS sls_order_dt,
	CASE 
		WHEN TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112) IS NOT NULL 
			THEN TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112)
		ELSE NULL 
	END AS sls_ship_dt,
	CASE 
		WHEN TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112) IS NOT NULL 
			THEN TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112)
		ELSE NULL 
	END AS sls_due_dt,

	CASE WHEN sls_sales IS NULL THEN (sls_quantity * sls_price)
		 WHEN sls_sales = 0 THEN  (sls_quantity * sls_price)
		 WHEN sls_sales < 0 THEN ABS(sls_sales)
		 WHEN sls_sales != (sls_quantity * ABS(sls_price)) THEN (sls_quantity * ABS(sls_price))
		 ELSE sls_sales
	END AS sls_sales,

	sls_quantity,

	CASE WHEN sls_price IS NULL THEN (sls_sales /  ISNULL(sls_quantity, 0))
		 WHEN sls_price = 0 THEN  (sls_sales / ISNULL(sls_quantity, 0))
		 WHEN sls_price < 0 THEN ABS(sls_price)
		 ELSE sls_price
	END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	  OR sls_sales <= 0 
	  OR sls_quantity <= 0 
	  OR sls_price <= 0
	  OR sls_sales IS NULL 
	  OR sls_quantity IS NULL 
	  OR sls_price IS NULL 
ORDER BY sls_sales, sls_quantity, sls_price


--SELECT * FROM silver.crm_sales_details




PRINT('============================================================================')
PRINT('Bronze.erp_cust_az12')
PRINT('============================================================================')
PRINT '>> Truncating Table: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;


--- Identify Out-of-Range Dates
--- Expectation: Birthdates between 1924-01-01 and Today

--CLEAN THE CID FOR MAYCH WITH OTHER COLUMNS
SELECT 
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1928-01-01' OR bdate > GETDATE()

--FINAL RESULT
SELECT 
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,
	gen
FROM bronze.erp_cust_az12



-- Data Standardization & Consistency
--1.
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

--2.
SELECT 
	DISTINCT gen ,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12

--3.
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,

	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,

	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12

--SELECT * FROM silver.erp_cust_az12




PRINT('============================================================================')
PRINT('Bronze.erp_loc_a101')
PRINT('============================================================================')
PRINT '>> Truncating Table: silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;




--- Data Standardization & Consistency

--1.CLEAN THE CID FOR MAYCH WITH OTHER COLUMNS
SELECT 
	REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101

--2.
SELECT 
	   REPLACE(cid, '-', '') AS cid,
	   CASE WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
			WHEN UPPER(TRIM(cntry)) IN ('US','USA','United States') THEN 'United States'
			WHEN UPPER(TRIM(cntry)) IN ('DE', 'Germany') THEN 'Germany'
			ELSE TRIM(cntry)
	   END AS cntry
FROM bronze.erp_loc_a101


--select * FROM silver.erp_loc_a101




PRINT('============================================================================')
PRINT('Bronze.erp_px_cat_g1v2')
PRINT('============================================================================')
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2;



--DON'T FOUND ANY DATA NEEDING FOR CLEANNING

SELECT 
	id,
	cat,
	subcat,
	maintenance 
FROM bronze.erp_px_cat_g1v2

--select * from silver.erp_px_cat_g1v2


