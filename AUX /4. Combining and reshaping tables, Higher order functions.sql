-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### - Handling JSON
-- MAGIC
-- MAGIC   use **':' (colon)** notation to access the data
-- MAGIC |ID|Cars|
-- MAGIC   |-|-|
-- MAGIC |Covers|{'Name':'chevrolet chevelle malibu', 'Miles_per_Gallon':18}|
-- MAGIC
-- MAGIC In above example 'Cars' is a json string type.
-- MAGIC
-- MAGIC Cars:Name = 'chevrolet chevelle malibu'
-- MAGIC
-- MAGIC   **schema_of_json(String)** - used to parse and extract JSON string and infer their schema 
-- MAGIC
-- MAGIC   **from_json(String, schema)** - used to read a json string
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### - Handling STRUCTURES
-- MAGIC use **'.' (dot)** notation to access the data
-- MAGIC
-- MAGIC |ID|Cars|
-- MAGIC   |-|-|
-- MAGIC |Covers|{'Name':'chevrolet chevelle malibu', 'Miles_per_Gallon':18}|
-- MAGIC
-- MAGIC In above example 'Cars' is a struct type.
-- MAGIC
-- MAGIC Cars.Name = 'chevrolet chevelle malibu'
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### - Handling ARRAYS
-- MAGIC Use **explode** function to flatten the array. 
-- MAGIC Example:
-- MAGIC
-- MAGIC Items: [{"Item_1": "Covers", "Item_2": "Mat", "Item_3": "Sterio"}, {"Item_1": "Mat", "Item_2": "Wheels", "Item_3": "Sterio"},{"Item_1": "Covers", "Item_2": "Tow-bar", "Item_3": "Camera"}]
-- MAGIC
-- MAGIC | Item_1 | Item_2 | Item_3 |
-- MAGIC |-|-|-|
-- MAGIC |Covers|Mat|Sterio|
-- MAGIC |Mat|Wheels|Sterio|
-- MAGIC |Covers|Two-bars|Camera|
-- MAGIC
-- MAGIC The **collect_set** function can collect unique values for a field, including fields within arrays. Example:
-- MAGIC
-- MAGIC Items: [{"Item_1": "Covers", "Item_2": "Mat", "Item_3": "Sterio"}, {"Item_1": "Mat", "Item_2": "Wheels", "Item_3": "Sterio"},{"Item_1": "Covers", "Item_2": "Tow-bar", "Item_3": "Camera"}]
-- MAGIC
-- MAGIC collect_set(Item_2) AS Items
-- MAGIC
-- MAGIC Result: [Mat, Wheels, Tow-bar]
-- MAGIC
-- MAGIC The **flatten** function allows multiple arrays to be combined into a single array.
-- MAGIC array_distinct(flatten(collect_set(struct_string.Items.Item_2))) AS Items
-- MAGIC
-- MAGIC Input: [[Mat, Wheels][Tow-bar, Wheels]]
-- MAGIC
-- MAGIC Output: [Mat, Wheels, Tow-bar, Wheels]
-- MAGIC
-- MAGIC
-- MAGIC The **array_distinct** function removes duplicate elements from an array.
-- MAGIC Input: [Mat, Wheels, Tow-bar, Wheels]
-- MAGIC
-- MAGIC Output: [Mat, Wheels, Tow-bar]
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### - Higher Order Functions
-- MAGIC
-- MAGIC **FILTER** filters an array using the given lambda function.
-- MAGIC
-- MAGIC **EXISTS** tests whether a statement is true for one or more elements in an array. 
-- MAGIC
-- MAGIC **TRANSFORM** uses the given lambda function to transform all elements in an array.
-- MAGIC
-- MAGIC **REDUCE** takes two lambda functions to reduce the elements of an array to a single value by merging the elements into a buffer, and the apply a finishing function on the final buffer.
-- MAGIC
-- MAGIC

-- COMMAND ----------

--Creating a dataset to show Pyspark functions on json, structures and arrays
CREATE OR REPLACE TABLE tbl_cars (ID INT, Cars STRING);

INSERT INTO tbl_cars VALUES
(1,"{'Name':'chevrolet chevelle malibu', 'Miles_per_Gallon':18, 'Cylinders':8, 'Displacement':307, 'Horsepower':130, 'Weight_in_lbs':3504, 'Acceleration':12, 'Year':'1970-01-01', 'Origin':'USA', 'Items':[{'Item_1':'Covers', 'Item_2':'Mat', 'Item_3':'Sterio'},{'Item_1':'Covers', 'Item_2':'Wheel', 'Item_3':'Mat'}]}"),
(2,"{'Name':'buick skylark 320', 'Miles_per_Gallon':15, 'Cylinders':8, 'Displacement':350, 'Horsepower':165, 'Weight_in_lbs':3693, 'Acceleration':11.5, 'Year':'1970-01-01', 'Origin':'USA', 'Items':[{'Item_1':'Wheel', 'Item_2':'Tow-bar', 'Item_3':'Camera'}, {'Item_1':'Wheel', 'Item_2':'Tow-bar', 'Item_3':'Wheel'}]}"),
(3,"{'Name':'plymouth satellite', 'Miles_per_Gallon':18, 'Cylinders':8, 'Displacement':318, 'Horsepower':150, 'Weight_in_lbs':3436, 'Acceleration':11, 'Year':'1970-01-01', 'Origin':'USA', 'Items':[{'Item_1':'Wheel', 'Item_2':'Sterio', 'Item_3':'Mat'}]}"),
(4,"{'Name':'amc rebel sst', 'Miles_per_Gallon':16, 'Cylinders':8, 'Displacement':304, 'Horsepower':150, 'Weight_in_lbs':3433, 'Acceleration':12, 'Year':'1970-01-01', 'Origin':'USA', 'Items':[{'Item_1':'Sterio', 'Item_2':'Tow-bar', 'Item_3':'Mat'}]}"),
(5,"{'Name':'amc rebel sst', 'Miles_per_Gallon':16, 'Cylinders':8, 'Displacement':304, 'Horsepower':150, 'Weight_in_lbs':3433, 'Acceleration':12, 'Year':'1970-01-01', 'Origin':'USA', 'Items':[{'Item_1':'Sterio', 'Item_2':'Heater', 'Item_3':'Mat'}]}");

SELECT * FROM tbl_cars;

-- COMMAND ----------

--Accessing Json attribute values using ':'
SELECT ID, Cars, Cars:Name  FROM tbl_cars;

-- COMMAND ----------

--Using schema_of_json()
SELECT schema_of_json("{'Name':'buick skylark 320', 'Miles_per_Gallon':15, 'Cylinders':8, 'Displacement':350, 'Horsepower':165, 'Weight_in_lbs':3693, 'Acceleration':11.5, 'Year':'1970-01-01', 'Origin':'USA', 'Items':[{'Item_1':'Wheel', 'Item_2':'Tow-bar', 'Item_3':'Camera'}, {'Item_1':'Wheel', 'Item_2':'Tow-bar', 'Item_3':'Wheel'}]}")

-- COMMAND ----------

--Using from_json function to cast our value field to a struct type
CREATE OR REPLACE TEMPORARY VIEW vw_struct_cars AS
SELECT
  ID,
  from_json(
    Cars,
    schema_of_json(
      "{'Name':'buick skylark 320', 'Miles_per_Gallon':15, 'Cylinders':8, 'Displacement':350, 'Horsepower':165, 'Weight_in_lbs':3693, 'Acceleration':11.5, 'Year':'1970-01-01', 'Origin':'USA', 'Items':[{'Item_1':'Wheel', 'Item_2':'Tow-bar', 'Item_3':'Camera'}, {'Item_1':'Wheel', 'Item_2':'Tow-bar', 'Item_3':'Wheel'}]}"
    )
  ) as struct_string
FROM
  tbl_cars;

-- COMMAND ----------

SELECT * FROM vw_struct_cars;

-- COMMAND ----------

DESCRIBE TABLE vw_struct_cars;

-- COMMAND ----------

--Using . dot notation to flatten the struct type attributes
SELECT ID, struct_string.Name, struct_string.Acceleration FROM vw_struct_cars;

-- COMMAND ----------

--using * to view all the fields from a struct
SELECT ID, struct_string.* FROM vw_struct_cars;

-- COMMAND ----------

--Using explode to separate rows, one for each item in the array.
SELECT ID, Name, Items.* FROM
(
SELECT ID,struct_string.Name,  explode(struct_string.Items) as Items  FROM vw_struct_cars
);

-- COMMAND ----------

SELECT struct_string.Name AS Cars,
  struct_string.Items AS Items
FROM vw_struct_cars

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The collect_set function can collect unique values for a field, including fields within arrays.
-- MAGIC
-- MAGIC The flatten function allows multiple arrays to be combined into a single array.
-- MAGIC
-- MAGIC The array_distinct function removes duplicate elements from an array.

-- COMMAND ----------

--Collect Item_2 from the Array
SELECT struct_string.Name AS Cars,
  collect_set(struct_string.Items.Item_2) AS Items
FROM vw_struct_cars
GROUP BY struct_string.Name

-- COMMAND ----------

--Removing duplicates by taking distinct and joining multiple arrays by using flatten
SELECT struct_string.Name AS Cars,
  array_distinct(flatten(collect_set(struct_string.Items.Item_2))) AS Items
FROM vw_struct_cars
GROUP BY struct_string.Name

-- COMMAND ----------

SELECT Id, struct_string.Items FROM vw_struct_cars;

-- COMMAND ----------

--Applying filter to get list of items where Item_1 = Wheel and getting the output in a new column
SELECT Id, struct_string.Items,
FILTER (struct_string.Items, i -> i.Item_1 = "Wheel") AS Item_With_Wheels
 FROM vw_struct_cars;

-- COMMAND ----------

--Removing the empty arrays
SELECT Id, Item_With_Wheels FROM 
(SELECT Id, struct_string.Items,
FILTER (struct_string.Items, i -> i.Item_1 = "Wheel") AS Item_With_Wheels
 FROM vw_struct_cars)
WHERE size(Item_With_Wheels) > 0;

-- COMMAND ----------

SELECT Id, struct_string.Items,
EXISTS (struct_string.Items, i -> i.Item_1 = "Wheel") AS Item_With_Wheels
 FROM vw_struct_cars;

-- COMMAND ----------

--Transforming an array and generating a new array as a result or Transform
SELECT Id, 
TRANSFORM (struct_string.Items, i -> CONCAT(i.Item_1, ", ", i.Item_2, ", ", i.Item_3)) AS Item_With_Tyres
 FROM vw_struct_cars;
