CREATE TABLE gold_dim_customer (
    customer_key    INT PRIMARY KEY,
    customer_id     INT,
    customer_number VARCHAR(50),
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    country         VARCHAR(50),
    gender          VARCHAR(50),
    birthdate       DATE,
    create_date     DATE
);

CREATE TABLE gold_dim_product (
    product_key    INT PRIMARY KEY,
    product_id     INT,
    product_number VARCHAR(50),
    product_name   VARCHAR(50),
    category_id    VARCHAR(50),
    category       VARCHAR(50),
    subcategory    VARCHAR(50),
    maintenance    VARCHAR(50),
    cost           INT,
    product_line   VARCHAR(50),
    start_date     DATE
);


CREATE TABLE gold_fact_sale (
    order_number   VARCHAR(50),
    product_key    INT,
    customer_key   INT,
    order_date     DATE,
    shipping_date  DATE,
    due_date       DATE,
    sales_amount   INT,
    quantity       INT,
    price          INT,
    FOREIGN KEY (product_key) REFERENCES gold_dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES gold_dim_customer(customer_key)
);


INSERT INTO gold_dim_customer (
    customer_key,
    customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    gender,
    birthdate,
    create_date
)

SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')  			   
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver_crm_cust_info ci
LEFT JOIN silver_erp_cust ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 la
    ON ci.cst_key = la.cid;


INSERT INTO gold_dim_product (
    product_key,
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    cost,
    product_line,
    start_date
)

SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver_crm_prd_info pn
LEFT JOIN silver_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;



INSERT INTO gold_fact_sale (
    order_number,
    product_key,
    customer_key,
    order_date,
    shipping_date,
    due_date,
    sales_amount,
    quantity,
    price
)

SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver_crm_sales_details sd
LEFT JOIN gold_dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;



CREATE VIEW country_sale AS
SELECT 
    c.country, 
    SUM(f.sales_amount) AS total_sales
FROM gold_fact_sales f
JOIN gold_dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;

CREATE VIEW customer_sale AS
SELECT 
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold_fact_sales f
JOIN gold_dim_customers c ON f.customer_key = c.customer_key
GROUP BY customer_name
ORDER BY total_revenue DESC
LIMIT 5;

CREATE VIEW product_sale AS
SELECT 
    p.product_name, 
    SUM(f.quantity) AS total_units_sold
FROM gold_fact_sales f
JOIN gold_dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_units_sold DESC
LIMIT 10;


WITH repeat_customers AS (
    SELECT customer_key
    FROM gold_fact_sales
    GROUP BY customer_key
    HAVING COUNT(DISTINCT order_number) > 1
)
SELECT 
    (COUNT(DISTINCT r.customer_key) * 100.0) / COUNT(DISTINCT f.customer_key) AS retention_rate
FROM gold_fact_sales f
LEFT JOIN repeat_customers r ON f.customer_key = r.customer_key;

CREATE VIEW category_sale AS
SELECT 
    p.category, 
    SUM(f.sales_amount) AS total_sales
FROM gold_fact_sales f
JOIN gold_dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;

