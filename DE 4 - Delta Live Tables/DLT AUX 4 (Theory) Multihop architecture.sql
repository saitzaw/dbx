-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Multihop/Medallion Architecture
-- MAGIC The **medallion architecture** describes a series of data layers that denote the quality of data stored in the lakehouse. 
-- MAGIC Databricks recommends taking a **multi-layered approach** to building a single source of truth.
-- MAGIC
-- MAGIC ####Advantages:
-- MAGIC * Atomicity
-- MAGIC * Consistency
-- MAGIC * Isolation
-- MAGIC * Durability
-- MAGIC
-- MAGIC ####Key points
-- MAGIC * Goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://www.databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Bronze Layer
-- MAGIC The Bronze layer is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "as-is," along with any additional metadata columns that capture the load date/time, process ID, etc.
-- MAGIC
-- MAGIC #####Key Points
-- MAGIC * Maintains the raw state of the data source.
-- MAGIC * Additional metadata (such as source file names or recording the time data was processed) may be added 
-- MAGIC * Can be any combination of streaming and batch transactions
-- MAGIC * Is appended incrementally and grows over time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Silver Layer
-- MAGIC Silver layer (cleansed and conformed data) In the Silver layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions.
-- MAGIC
-- MAGIC #####Key point(s)
-- MAGIC * Validate and deduplicate data in the **silver layer**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Gold layer
-- MAGIC Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins.
-- MAGIC
-- MAGIC #####Key Points
-- MAGIC * highly refined and aggregate, curated business-level tables
-- MAGIC * The Gold layer is for reporting
