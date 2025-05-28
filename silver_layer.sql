CREATE TABLE silver_crm_cust_info (
cst_id INT,
cst_key VARCHAR(50) PRIMARY KEY,
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_material_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
)


CREATE TABLE silver_crm_prd_info (
prd_id INT PRIMARY KEY,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
)

CREATE TABLE silver_crm_sales_details (
sls_ord_num VARCHAR(50) PRIMARY Key,
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


CREATE TABLE silver_erp_cust (
	CID VARCHAR(50) PRIMARY KEY,
	BDATE DATE,
	GEN VARCHAR(50),
	ADD CONSTRAINT fk_erp_cust FOREIGN KEY (CID) 
	REFERENCES silver_crm_cust_info (cst_key)
)

CREATE TABLE silver_erp_loc_a101(
	CID VARCHAR(50) PRIMARY KEY,
	CNTRY VARCHAR(50),
	ADD CONSTRAINT fk_erp_loc FOREIGN KEY (CID) 
	REFERENCES silver_crm_cust_info (cst_key)
)

CREATE TABLE silver_px_cat_g1v2 (
	ID VARCHAR(50) PRIMARY KEY,
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50)
)



INSERT INTO silver_crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date
)

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN cst_marital_status = 'M' THEN 'Married'
	 WHEN cst_marital_status = 'S' THEN 'Single'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN cst_gndr = 'F' THEN 'Female'
	 WHEN cst_gndr = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
CAST(cst_create_date AS DATE) AS cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM crm_cust_info
)t WHERE flag_last = 1;


INSERT INTO silver_crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
prd_nm,
COALESCE(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt::DATE) OVER (PARTITION BY prd_key ORDER BY prd_start_dt::DATE) - INTERVAL '1 day' AS DATE) AS prd_end_dt
FROM crm_prd_info;



INSERT INTO silver_crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)


SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD') 
    END AS sls_order_dt,
    
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD') 
    END AS sls_ship_dt,

    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD') 
    END AS sls_due_dt,

    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM crm_sales_details;


INSERT INTO silver_erp_cust(
cid,
bdate,
gen
)

SELECT
    CASE 
        WHEN "CID" LIKE 'NAS%' THEN SUBSTRING("CID", 4, LENGTH("CID")) 
        ELSE "CID" 
    END AS cid,
    CASE 
        WHEN TO_DATE("BDATE", 'YYYY-MM-DD') > CURRENT_DATE THEN NULL 
        ELSE TO_DATE("BDATE", 'YYYY-MM-DD') 
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM("GEN")) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM("GEN")) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM "erp_CUST_AZ12";
SELECT * FROM silver_erp_cust;



INSERT INTO silver_erp_loc_a101(
cid,
cntry
)
SELECT
REPLACE("CID", '-', '') AS cid, 
CASE
WHEN TRIM("CNTRY") = 'DE' THEN 'Germany'
WHEN TRIM("CNTRY") IN ('US', 'USA') THEN 'United States'
WHEN TRIM("CNTRY") = '' OR "CNTRY" IS NULL THEN 'n/a'
ELSE TRIM("CNTRY")
END AS cntry
FROM "erp_LOC_A101";



INSERT INTO silver_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
)

SELECT
"ID",
"CAT",
"SUBCAT",
"MAINTENANCE"
FROM "erp_PX_CAT_G1V2";
SELECT * FROM silver_px_cat_g1v2;
