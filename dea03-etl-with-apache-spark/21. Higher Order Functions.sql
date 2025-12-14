-- Databricks notebook source
-- TRANSFORM
-- FILTER
-- EXISTS 
-- AGGREGATE

-- COMMAND ----------

-- Syntax:
-- <function_name> (array_column, lambda_expression)
-- lambda_expression element -> expression

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items AS
SELECT * FROM VALUES
        (1, array('smartphone', 'laptop', 'monitor')),
        (2, array('tablet', 'headphones', 'smartwatch')),
        (3, array('keyboard', 'mouse'))
AS orders(order_id, items);

-- COMMAND ----------

SELECT * FROM order_items;

-- COMMAND ----------

SELECT order_id,
        TRANSFORM(items, x -> UPPER(x)) AS upper_items
FROM order_items;

-- COMMAND ----------

SELECT order_id,
        FILTER(items, x -> x LIKE '%smart%') AS upper_items
FROM order_items;

-- COMMAND ----------

SELECT order_id,
        EXISTS(items, x -> x  == 'smartwatch') AS upper_items
FROM order_items;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT * FROM VALUES
(1, array(
named_struct('name', 'smartphone', 'price', 699),
named_struct('name', 'laptop', 'price', 1199),
named_struct('name', 'monitor', 'price', 399)
)),
(2, array(
named_struct('name', 'tablet', 'price', 599),
named_struct('name', 'headphones', 'price', 199),
named_struct('name', 'smartwatch', 'price', 299)
)),
(3, array(
named_struct('name', 'keyboard', 'price', 89),
named_struct('name', 'mouse', 'price', 59)
))
AS orders(order_id, items);


-- COMMAND ----------

SELECT * FROM order_items;

-- COMMAND ----------

SELECT order_id,
       TRANSFORM(items, x -> named_struct(
                                          'name', UPPER(x.name),
                                          'price', ROUND(x.price * 1.1, 2)
       ))  AS items_with_tax
FROM order_items;

-- COMMAND ----------

SELECT order_id,
       AGGREGATE(items, 0, (acc,x) -> acc + x.price)  AS items_with_tax
FROM order_items;

-- COMMAND ----------

-- Common used Higher Order map Functions
-- TRANSFORM_VALUES
-- TRANSFORM_KEYS
-- MAP_FILTER

-- COMMAND ----------

-- Syntax
-- <function_name> (map_col, lambda_exp)
-- lambda expression: (key, value) -> expression

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW order_item_prices AS
SELECT * FROM VALUES
(1, map('smartphone', 699, 'laptop', 1199, 'monitor', 399)),
(2, map('tablet', 599, 'headphones', 199, 'smartwatch', 299)),
(3, map('keyboard', 89, 'mouse', 59))
AS orders(order_id, item_prices);

-- COMMAND ----------

SELECT * FROM order_item_prices;

-- COMMAND ----------

SELECT order_id,
        TRANSFORM_KEYS(item_prices, (item, price) -> UPPER(item)) AS items_upper_case
FROM order_item_prices;

-- COMMAND ----------

SELECT order_id,
        TRANSFORM_VALUES(item_prices, (item, price) -> ROUND(price * 1.1, 2)) AS items_upper_case
FROM order_item_prices

-- COMMAND ----------

SELECT order_id,
        MAP_FILTER(item_prices, (item, price) -> price > 500) AS premium_items
FROM order_item_prices