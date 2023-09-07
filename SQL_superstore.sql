use proyecto;

SELECT * FROM dbo.Orders;

SELECT COUNT (*) AS Total_registros FROM dbo.Orders;

-- La base de datos adjuntada cuenta con un total de 9994 registros. Se realizará un proceso de normalización,
-- donde se subdividirán los datos en diferentes tablas según un modelo snowflake de dimensiones y hechos.

-- Creación de la tabla dbo.dim_shipment
SELECT DISTINCT ship_mode
INTO dim_shipment
FROM dbo.Orders;

ALTER TABLE dbo.dim_shipment
ADD class_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_shipment
ADD PRIMARY KEY (class_id);

SELECT * FROM dbo.dim_shipment;

-- Creación de la tabla dbo.dim_order

SELECT DISTINCT B.order_id, B.order_date, B.ship_date, A.class_id
INTO dim_order
FROM dbo.Orders B
INNER JOIN dbo.dim_shipment A
ON B.ship_mode = A.ship_mode;

ALTER TABLE dbo.dim_order
ADD row_order_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_order
ADD PRIMARY KEY (row_order_id);

ALTER TABLE dbo.dim_order
ADD FOREIGN KEY (class_id) REFERENCES dbo.dim_shipment (class_id);

SELECT * FROM dbo.dim_order;

-- Creación de la tabla dbo.dim_category

SELECT DISTINCT category
INTO dim_category
FROM dbo.Orders;

ALTER TABLE dbo.dim_category
ADD category_id int IDENTITY (1,1);

ALTER TABLE dbo.dim_category
ADD PRIMARY KEY (category_id);

SELECT * FROM dbo.dim_category;

-- Creación de la tabla dbo.dim_subcategory

SELECT DISTINCT B.subcategory, A.category_id
INTO dim_subcategory
FROM dbo.Orders B
INNER JOIN dbo.dim_category A
ON B.category = A.category;

ALTER TABLE dbo.dim_subcategory
ADD sub_id int IDENTITY (1,1);

ALTER TABLE dbo.dim_subcategory
ADD PRIMARY KEY (sub_id);

ALTER TABLE dbo.dim_subcategory
ADD FOREIGN KEY (category_id) REFERENCES dbo.dim_category (category_id);

SELECT * FROM dbo.dim_subcategory;

-- Creación de la tabla dbo.dim_product

SELECT DISTINCT B.product_name, A.sub_id
INTO dim_product
FROM dbo.Orders B
INNER JOIN dbo.dim_subcategory A
ON B.subcategory = A.subcategory;

SELECT * FROM dbo.dim_product;

		-- Se encontró una duplicación de datos, por lo que se ejecuta la siguiente consulta.

WITH A AS (
SELECT product_name, sub_id,
ROW_NUMBER() OVER (PARTITION BY
product_name ORDER BY product_name) AS duplicado
FROM dbo.dim_product
)
DELETE FROM A
WHERE duplicado > 1;


		-- Una vez eliminados los duplicados se continua con el proceso de cardinalidad.

ALTER TABLE dbo.dim_product
ADD product_id int IDENTITY (1000,1) NOT NULL;

ALTER TABLE dbo.dim_product
ADD PRIMARY KEY (product_id);

ALTER TABLE dbo.dim_product
ADD FOREIGN KEY (sub_id) REFERENCES dbo.dim_subcategory (sub_id);

SELECT * FROM dbo.dim_product;

-- Creación de la tabla dbo.dim_segment

SELECT DISTINCT segment
INTO dim_segment
FROM dbo.Orders;

ALTER TABLE dbo.dim_segment
ADD segment_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_segment
ADD PRIMARY KEY (segment_id);

SELECT * FROM dbo.dim_segment;

-- Creación de la tabla dbo.dim_customer

SELECT DISTINCT B.customer_name, A.segment_id
INTO dim_customer
FROM dbo.Orders B
INNER JOIN dbo.dim_segment A
ON B.segment = A.segment;

ALTER TABLE dbo.dim_customer
ADD customer_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_customer
ADD PRIMARY KEY (customer_id);

ALTER TABLE dbo.dim_customer
ADD FOREIGN KEY (segment_id) REFERENCES dbo.dim_segment (segment_id);

SELECT * FROM dbo.dim_customer;

-- Creación de la tabla dbo.dim_country

SELECT DISTINCT country
INTO dim_country
FROM dbo.Orders;

ALTER TABLE dbo.dim_country
ADD country_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_country
ADD PRIMARY KEY (country_id);

-- Creación de la tabla dbo.dim_region

SELECT DISTINCT B.region, A.country_id
INTO dim_region
FROM dbo.Orders B
INNER JOIN dbo.dim_country A
ON B.country = A.country;

ALTER TABLE dbo.dim_region
ADD region_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_region
ADD PRIMARY KEY (region_id);

ALTER TABLE dbo.dim_region
ADD FOREIGN KEY (country_id) REFERENCES dbo.dim_country (country_id);

SELECT * FROM dbo.dim_region;

-- Creación de la tabla dbo.dim_state

SELECT DISTINCT B.state, A.region_id
INTO dim_state
FROM dbo.orders B
INNER JOIN dbo.dim_region A
ON B.region = A.region;

ALTER TABLE dbo.dim_state
ADD state_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_state
ADD PRIMARY KEY (state_id);

ALTER TABLE dbo.dim_state
ADD FOREIGN KEY (region_id) REFERENCES dbo.dim_region (region_id);

SELECT * FROM dbo.dim_state;

-- Creación de la tabla dbo.dim_city

SELECT DISTINCT B.city, A.state_id
INTO dim_city
FROM dbo.orders B
INNER JOIN dbo.dim_state A
ON B.state = A.state;

SELECT * FROM dbo.dim_city;

		-- Se encontraron datos duplicados.

WITH B AS (
SELECT city, state_id,
ROW_NUMBER() OVER (PARTITION BY
city ORDER BY city) AS duplicado
FROM dbo.dim_city
)
DELETE FROM B
WHERE duplicado > 1;

		-- Se realiza la inserción de claves primarias y foráneas.

ALTER TABLE dbo.dim_city
ADD city_id int IDENTITY (1,1) NOT NULL;

ALTER TABLE dbo.dim_city
ADD PRIMARY KEY (city_id);

ALTER TABLE dbo.dim_city
ADD FOREIGN KEY (state_id) REFERENCES dbo.dim_state (state_id);

SELECT * FROM dbo.dim_city;

-- Creación de la tabla dbo.fact_sales

SELECT DISTINCT A.row_id, A.sales, A.quantity, A.discount, A.profit, B.row_order_id, C.product_id, D.customer_id, E.city_id
INTO fact_sales
FROM dbo.Orders A
	INNER JOIN dbo.dim_order B
	ON A.order_id = B.order_id
	INNER JOIN dbo.dim_product C
	ON A.product_name = C.product_name
	INNER JOIN dbo.dim_customer D
	ON A.customer_name = D.customer_name
	INNER JOIN dbo.dim_city E
	ON A.city = E.city
ORDER BY row_id;

ALTER TABLE dbo.fact_sales
ALTER COLUMN row_id int NOT NULL;

ALTER TABLE dbo.fact_sales
ADD PRIMARY KEY (row_id);

ALTER TABLE dbo.fact_sales
ADD FOREIGN KEY (row_order_id) REFERENCES dbo.dim_order (row_order_id);

ALTER TABLE dbo.fact_sales
ADD FOREIGN KEY (product_id) REFERENCES dbo.dim_product (product_id);

ALTER TABLE dbo.fact_sales
ADD FOREIGN KEY (customer_id) REFERENCES dbo.dim_customer (customer_id);

ALTER TABLE dbo.fact_sales
ADD FOREIGN KEY (city_id) REFERENCES dbo.dim_city (city_id);