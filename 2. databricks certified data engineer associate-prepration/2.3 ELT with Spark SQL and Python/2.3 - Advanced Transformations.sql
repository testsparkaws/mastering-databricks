-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

customers
----------
* customer_id(PK)
* email
* profile 
* updated

Orders
-----
* order_id (PK)
* timestamp
* customer_id(FK)
* quantity 
* total 
* books (FK)

books 
-----
* book_id(PK)
* title 
* author 
* category 
* price 

-- COMMAND ----------
%run ../Includes/Copy-Datasets

-- COMMAND ----------
SELECT * FROM customers

-- COMMAND ------
DESCRIBE customers

-- COMMAND ----------
SELECT customer_id, profile:first_name, profile:address:country FROM customers


-- COMMAND ----------
SELECT from_json(profile) AS profile_struct FROM customers;

-- COMMAND ----------
SELECT profile FROM customers LIMIT 1

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, 
       schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":
         {"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')
      ) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers

-- COMMAND ----------
DESCRIBE parsed_customers

-- COMMAND ----------
SELECT customer_id, profile_struct.first_name, profile_struct.address.country FROM parsed_customers


-- COMMAND: Flatten ----------
CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND : Array of Books ----------
SELECT order_id, customer_id, books FROM orders

-- COMMAND : explode the array and each ----------
SELECT order_id, customer_id, explode(books) AS book FROM orders

-- COMMAND ----------
SELECT 
  customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set

FROM orders
GROUP BY customer_id

-- COMMAND ----------
SELECT 
  customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND: JOIN ----------
CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

-- SET UNIION
SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 

-- COMMAND: SET Intersection  ----------
SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 

-- COMMAND (A - B)----------
SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------
CREATE OR REPLACE TABLE transactions AS
SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
