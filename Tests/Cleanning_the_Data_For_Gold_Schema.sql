
-- Check if find any duplicate
SELECT cst_id, COUNT(*) 
FROM(
	SELECT 
		ci.cst_id,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
			ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 AS la
			ON ci.cst_key = la.cid
	) AS t 
GROUP BY cst_id
HAVING COUNT(*) > 1


--CLEANNING MISSING VALUES IN GENDER 
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
	END AS new_gen,

	ci.cst_create_date,
	ca.bdate,
	la.cntry
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
		ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
		ON ci.cst_key = la.cid


--RENAME COLUMNS FOR FRIENDLY SYSTEM
SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name ,
	la.cntry AS country ,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
		ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
		ON ci.cst_key = la.cid
WHERE ci.cst_id IS NOT NULL AND ci.cst_key IS NOT NULL;


--INSERT PRIMARY KEY 
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name ,
	la.cntry AS country ,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
		ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
		ON ci.cst_key = la.cid
WHERE ci.cst_id IS NOT NULL AND ci.cst_key IS NOT NULL;



--CREATE VIEW
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name ,
	la.cntry AS country ,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
		ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
		ON ci.cst_key = la.cid
WHERE ci.cst_id IS NOT NULL AND ci.cst_key IS NOT NULL;


SELECT * FROM gold.dim_customers




print('=======================================================')




--FILTER OUT ALL HISTORICAL DATA FROM PRD_END_DATE COLUMN
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt
FROM silver.crm_prd_info AS pn
WHERE pn.prd_end_dt IS NULL 


-- Check if find any duplicate
SELECT prd_key, COUNT(*)
FROM(
	SELECT 
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM silver.crm_prd_info AS pn
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc
			ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL 
) AS t
GROUP BY prd_key
HAVING COUNT(*) > 1 


--RENAME COLUMNS FOR FRIENDLY SYSTEM
SELECT 
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.cat AS category,
		pc.subcat AS subcategory,
		pc.maintenance AS maintenance,
		pn.prd_cost AS cost,
		pn.prd_line AS product_line,
		pn.prd_start_dt AS start_date 
	FROM silver.crm_prd_info AS pn
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc
			ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL 

--CREATE PRIMARY KEY 
SELECT 
		ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.cat AS category,
		pc.subcat AS subcategory,
		pc.maintenance AS maintenance,
		pn.prd_cost AS cost,
		pn.prd_line AS product_line,
		pn.prd_start_dt AS start_date 
	FROM silver.crm_prd_info AS pn
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc
			ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL 



--CREATE VIEW
CREATE VIEW gold.dim_products AS 
SELECT 
		ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.cat AS category,
		pc.subcat AS subcategory,
		pc.maintenance AS maintenance,
		pn.prd_cost AS cost,
		pn.prd_line AS product_line,
		pn.prd_start_dt AS start_date 
	FROM silver.crm_prd_info AS pn
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc
			ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL 

SELECT * FROM gold.dim_products




PRINT('=====================================================================')



--
SELECT 
	sd.sls_ord_num,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
		ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
		ON sd.sls_cust_id = cu.customer_id



--RENAME COLUMNS FOR FRIENDLY SYSTEM
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
		ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
		ON sd.sls_cust_id = cu.customer_id



--CREATE VIEW
CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
		ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
		ON sd.sls_cust_id = cu.customer_id

SELECT * FROM gold.fact_sales



PRINT('=====================================================')


--FOREIGN KEY INTEGRITY (DIMENSIONS)
SELECT * FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
		ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p
		ON f.product_key = p.product_key
WHERE c.customer_key IS NULL AND p.product_key IS NULL


