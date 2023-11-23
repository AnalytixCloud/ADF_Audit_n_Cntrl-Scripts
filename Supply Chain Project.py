# Databricks notebook source
adls_key = dbutils.secrets.get(scope = "key-vault-secret", key = "storageaccountkeys") 

# COMMAND ----------

storageAccountName = "adlssupplychaindev01"
blobContainerName = "raw"
mountPoint = "/mnt/raw/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:w
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': adls_key}
      #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

storageAccountName = "adlssupplychaindev01"
blobContainerName = "silver"
mountPoint = "/mnt/silver/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': adls_key}
      #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

storageAccountName = "adlssupplychaindev01"
blobContainerName = "bronze"
mountPoint = "/mnt/bronze/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': adls_key}
      #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

storageAccountName = "adlssupplychaindev01"
blobContainerName = "gold"
mountPoint = "/mnt/gold/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': adls_key}
      #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

# MAGIC %fs ls mnt/raw/

# COMMAND ----------

# MAGIC %md #Bronze

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Read supplier data
supplier_df = spark.read.parquet('/mnt/raw/supplier_data.parquet', header=True, inferSchema=True)

# Read transportation data
transportation_df = spark.read.parquet('/mnt/raw/transportation.parquet', header=True, inferSchema=True)

# Read product data
product_df = spark.read.parquet('/mnt/raw/product_table.parquet', header=True, inferSchema=True)

# Read shipping data
purchaseorder_df = spark.read.parquet('/mnt/raw/purchaseorder.parquet', header=True, inferSchema=True)

# Data Quality Framework - Total null counts for each table
supplier_null_counts = supplier_df.select([count(when(col(c).isNull(), c)).alias(c+"_null_count") for c in supplier_df.columns])
transportation_null_counts = transportation_df.select([count(when(col(c).isNull(), c)).alias(c+"_null_count") for c in transportation_df.columns])
product_null_counts = product_df.select([count(when(col(c).isNull(), c)).alias(c+"_null_count") for c in product_df.columns])
purchaseorder_null_counts = purchaseorder_df.select([count(when(col(c).isNull(), c)).alias(c+"_null_count") for c in purchaseorder_df.columns])

# Display total null counts for each table
# Display total null counts for each table using display function
display(supplier_null_counts)
display(transportation_null_counts)
display(product_null_counts)
display(purchaseorder_null_counts)




# COMMAND ----------

# Data Type Validation
def validate_data_types(data, table_name):
    print(f"Validating data types for {table_name}...")
    for field in data.schema.fields:
        column_name = field.name
        column_type = str(field.dataType)
        
        # Check for Integer type
        if "int" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("int")))
            print(f"{column_name}: Integer type validation applied.")
        
        # Check for Double type
        elif "double" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("double")))
            print(f"{column_name}: Double type validation applied.")
        
        # Check for String type
        elif "string" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("string")))
            print(f"{column_name}: String type validation applied.")
        
        # Check for Date type
        elif "date" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("date")))
            print(f"{column_name}: Date type validation applied.")
        
        # Check for Boolean type
        elif "boolean" in column_type.lower():
            data = data.withColumn(column_name, when(col(column_name).isNotNull(), col(column_name).cast("boolean")))
            print(f"{column_name}: Boolean type validation applied.")
        
        # Add more data types as needed
    
    print(f"Validation for {table_name} completed.\n")
    return data


# Validate data types for each table
supplier_data = validate_data_types(supplier_df, "supplier_data")
transportation_data = validate_data_types(transportation_df, "transportation_data")
product_data = validate_data_types(product_df, "product_data")
purchaseorder_data = validate_data_types(purchaseorder_df, "purchaseorder_data")


# COMMAND ----------

# Data Cleaning - Handling missing values
supplier_data = supplier_data.na.fill({"SubCity": "Unknown", "Telephone": "999999999"})
transportation_data = transportation_data.na.fill({"MODES": "Unknown"})
product_data = product_data.na.fill({"P_ID": "Unknown", "Product_type": "Unknown"})
purchaseorder_data = purchaseorder_data.na.fill({"PO_ID": 0, "P_ID": 0, "Order_quantities": 0})

# Data Enrichment - No additional data sources provided for simplicity

# Data Partitioning - Assuming no partitioning for simplicity
# Write the processed data to the Bronze Layer as Delta tables
supplier_data.write.format("delta").mode("overwrite").save("/mnt/bronze/supplier/supplier_data_delta")
transportation_data.write.format("delta").mode("overwrite").save("/mnt/bronze/transportation/transportation_data_delta")
product_data.write.format("delta").mode("overwrite").save("/mnt/bronze/product/product_data_delta")
purchaseorder_data.write.format("delta").mode("overwrite").save("/mnt/bronze/purchaseorder/purchaseorder_data_delta")


# COMMAND ----------

# MAGIC %md # Silver

# COMMAND ----------


# Read Delta tables from Bronze Layer
supplier_data = spark.read.format("delta").load("/mnt/bronze/supplier/supplier_data_delta")
transportation_data = spark.read.format("delta").load("/mnt/bronze/transportation/transportation_data_delta")
product_data = spark.read.format("delta").load("/mnt/bronze/product/product_data_delta")
purchaseorder_data = spark.read.format("delta").load("/mnt/bronze/purchaseorder/purchaseorder_data_delta")

# Business Rule Application (Silver Layer) - Supplier Data
supplier_data = supplier_data.withColumn("Negotiation_Score", 
                                         when(col("Business_Type") == "Printing Press", 0.9)
                                         .when(col("Business_Type") == "Stationery Materials", 0.8)
                                         .when(col("Business_Type") == "Software Development and Design", 0.95)
                                         .otherwise(0.75))

supplier_data = supplier_data.withColumn("Defect_Quality", 
                                         when(col("Business_Type").isin("Detergents", "Sanitary Items"), "High")
                                         .when(col("Business_Type").isin("Building and Construction Materials", 
                                                                       "Metal and Metal Products"), "Medium")
                                         .otherwise("Low"))

# Business Rule Application (Silver Layer) - Transportation Data
transportation_data = transportation_data.withColumn("Priority",
                                                     when(col("MODES") == "Truck", 1)
                                                     .when(col("MODES") == "Ship", 2)
                                                     .when(col("MODES") == "Airplane", 3)
                                                     .otherwise(0))


# Business Rule Application (Silver Layer) - Product Data
product_data = product_data.withColumn("Price_Category",
                                       when(col("Price") < 50, "Low Price")
                                       .when((col("Price") >= 50) & (col("Price") < 200), "Medium Price")
                                       .when(col("Price") >= 200, "High Price")
                                       .otherwise("Unknown"))


# Business Rule Application (Silver Layer) - Purchase Order Data
purchaseorder_data = purchaseorder_data.withColumn("Total_Cost", col("Order_quantities") * col("Costs"))


# COMMAND ----------

# Write the Silver Layer data to Delta Lake - Supplier Data
silver_layer_path_supplier = "/mnt/silver/supplier/"
supplier_data.write.format("delta").mode("overwrite").save(silver_layer_path_supplier)

# Write the Silver Layer data to Delta Lake - Transportation Data
silver_layer_path_transportation = "/mnt/silver/transportation/"
transportation_data.write.format("delta").mode("overwrite").save(silver_layer_path_transportation)

# Write the Silver Layer data to Delta Lake - Product Data
silver_layer_path_product = "/mnt/silver/product/"
product_data.write.format("delta").mode("overwrite").save(silver_layer_path_product)

# Write the Silver Layer data to Delta Lake - Purchase Order Data
silver_layer_path_purchaseorder = "/mnt/silver/purchaseorder/"
purchaseorder_data.write.format("delta").mode("overwrite").save(silver_layer_path_purchaseorder)

# COMMAND ----------

# MAGIC %md #Gold

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, desc, rank, when
from pyspark.sql.window import Window

# Read Silver Layer data for Supplier
silver_layer_supplier_path = "/mnt/silver/supplier/"
supplier_data_silver = spark.read.format("delta").load(silver_layer_supplier_path)

# Read Silver Layer data for Transportation
silver_layer_transportation_path = "/mnt/silver/transportation/"
transportation_data_silver = spark.read.format("delta").load(silver_layer_transportation_path)

# Read Silver Layer data for Product
silver_layer_product_path = "/mnt/silver/product/"
product_data_silver = spark.read.format("delta").load(silver_layer_product_path)

# Read Silver Layer data for Purchase Order
silver_layer_purchaseorder_path = "/mnt/silver/purchaseorder/"
purchaseorder_data_silver = spark.read.format("delta").load(silver_layer_purchaseorder_path)


# COMMAND ----------

display(supplier_data_silver)

# COMMAND ----------

# Advanced analytics:

# Example 1: Calculate the average number of services provided by suppliers
avg_services_per_supplier = supplier_data_silver.groupBy("Company_Name").agg(
    countDistinct("Business_Type").alias("Service_Count")
).agg({"Service_Count": "avg"}).collect()[0][0]

print(f"Average number of services provided by suppliers: {avg_services_per_supplier:.2f}")

# COMMAND ----------

# Example 2: Identify top suppliers by the number of materials provided
top_material_providers = supplier_data_silver.filter(col("Business_Type") == "Constrution materialsConstruction").groupBy("Company_Name").agg(
    countDistinct("Business_Type").alias("Material_Count")
).orderBy(desc("Material_Count")).limit(10)

top_material_providers.show()

# COMMAND ----------

# Example 3: Rank suppliers based on the total number of services provided
windowSpec = Window.orderBy(desc("Service_Count"))
ranked_suppliers = supplier_data_silver.groupBy("Company_Name").agg(
    countDistinct("Business_Type").alias("Service_Count")
).withColumn("Rank", rank().over(windowSpec))

ranked_suppliers.show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct, sum, avg, col,lit

# Define a list of columns to handle non-numeric values and cast to double
numeric_columns = ["Number_of_products_sold", "Revenue_generated", "Stock_levels", "Lead_times",
                   "Order_quantities", "Shipping_times", "Shipping_costs", "Total_Cost"]

# Ensure all specified columns are cast to double and handle non-numeric values
for column in numeric_columns:
    purchaseorder_data_silver = purchaseorder_data_silver.withColumn(
        column,
        col(column).cast("double")
    )

    # Filter out rows where the column is not numeric
    purchaseorder_data_silver = purchaseorder_data_silver.filter(
        col(column).isNotNull()
    )

# Perform aggregation
comprehensive_purchaseorder = purchaseorder_data_silver.groupBy("Supplier_Id").agg(
    countDistinct("PO_ID").alias("Distinct_PurchaseOrder_Count"),
    sum("Number_of_products_sold").alias("Total_Products_Sold"),
    sum("Revenue_generated").alias("Total_Revenue"),
    avg("Stock_levels").alias("Avg_Stock_Levels"),
    avg("Lead_times").alias("Avg_Lead_Times"),
    sum("Order_quantities").alias("Total_Order_Quantities"),
    avg("Shipping_times").alias("Avg_Shipping_Times"),
    countDistinct("Shipping_carriers").alias("Distinct_Shipping_Carriers"),
    avg("Shipping_costs").alias("Avg_Shipping_Costs"),
    sum("Total_Cost").alias("Total_Cost")
)

# Display the result
display(comprehensive_purchaseorder)


# COMMAND ----------

display(supplier_data_silver)

# COMMAND ----------

# Assuming the columns exist in comprehensive_purchaseorder
comprehensive_suppliers = supplier_data_silver.join(
    comprehensive_purchaseorder, 
    supplier_data_silver['S_ID'] == comprehensive_purchaseorder["Supplier_Id"], 
    how="inner"
)

# Calculate the Recommendation Score based on specific columns
comprehensive_suppliers = comprehensive_suppliers.withColumn(
    "Recommendation_Score",
    coalesce(col("Total_Products_Sold").cast("double"), lit(0)) +
    coalesce(col("Total_Revenue").cast("double"), lit(0)) +
    coalesce(col("Avg_Stock_Levels").cast("double"), lit(0)) + 
    coalesce(col("Avg_Lead_Times").cast("double"), lit(0)) +
    coalesce(col("Total_Order_Quantities").cast("double"), lit(0)) + 
    coalesce(col("Avg_Shipping_Times").cast("double"), lit(0)) +
    coalesce(col("Distinct_Shipping_Carriers").cast("double"), lit(0)) + 
    coalesce(col("Avg_Shipping_Costs").cast("double"), lit(0)) + 
    coalesce(col("Total_Cost").cast("double"), lit(0)) +
    coalesce(col("Negotiation_Score").cast("double"), lit(0)) +
    coalesce(col("Defect_Quality").cast("double"), lit(0))
)

# Create a ranking based on the Recommendation Score
windowSpec = Window.orderBy(desc("Recommendation_Score"))
comprehensive_suppliers = comprehensive_suppliers.withColumn(
    "Rank", 
    row_number().over(windowSpec)
)


# Display the result
comprehensive_suppliers.show()

# COMMAND ----------

# Specify the path for the gold layer
gold_layer_path = "/mnt/gold/comprehensive_suppliers"

# Write the DataFrame to the Delta table
comprehensive_suppliers.write.format("delta").mode("overwrite").save(gold_layer_path)


# COMMAND ----------

# SQL query to select data from the Delta table
sql_query = """
SELECT *
FROM delta.`/mnt/gold/comprehensive_suppliers`
"""

# Register the DataFrame as a temporary SQL table
comprehensive_suppliers.createOrReplaceTempView("comprehensive_suppliers_view")

# Run the SQL query
result_df = spark.sql(sql_query)



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM comprehensive_suppliers_view

# COMMAND ----------

# MAGIC %python
# MAGIC %pip install plotly
# MAGIC import matplotlib.pyplot as plt
# MAGIC
# MAGIC # Assuming you have a DataFrame called comprehensive_suppliers
# MAGIC # You can replace it with your actual DataFrame name
# MAGIC df = comprehensive_suppliers.toPandas()
# MAGIC
# MAGIC # Plot a bar chart
# MAGIC df.plot(kind='bar', x='Supplier_Id', y='Total_Revenue', title='Total Revenue by Supplier')
# MAGIC plt.show()
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import plotly.express as px
# MAGIC import pandas as pd
# MAGIC
# MAGIC # Assuming comprehensive_suppliers is your DataFrame
# MAGIC # Convert to Pandas DataFrame
# MAGIC pandas_df = comprehensive_suppliers.toPandas()
# MAGIC
# MAGIC # Sort the DataFrame by Recommendation_Score in descending order
# MAGIC sorted_df = pandas_df.sort_values(by='Recommendation_Score', ascending=False)
# MAGIC
# MAGIC # Specify the number of top suppliers you want to display
# MAGIC top_n_suppliers = 5  # Replace with the desired number
# MAGIC
# MAGIC # Select the top N suppliers
# MAGIC top_n_df = sorted_df.head(top_n_suppliers)
# MAGIC
# MAGIC # Convert Supplier_Id to a string to avoid scientific notation
# MAGIC top_n_df['Supplier_Id'] = top_n_df['Supplier_Id'].astype(str)
# MAGIC
# MAGIC # Create an interactive bar chart for the top N suppliers
# MAGIC fig = px.bar(
# MAGIC     top_n_df,
# MAGIC     x='Supplier_Id',
# MAGIC     y='Recommendation_Score',
# MAGIC     title=f'Top {top_n_suppliers} Suppliers Based on Recommendation Score',
# MAGIC     labels={'Recommendation_Score': 'Recommendation Score'},
# MAGIC     color='Business_Type',
# MAGIC     category_orders={'Supplier_Id': top_n_df['Supplier_Id'].tolist()}  # Set category order
# MAGIC )
# MAGIC
# MAGIC fig.show()
# MAGIC

# COMMAND ----------

# MAGIC %md # Thank You - Please do subscribe Analytix Cloud for more such projects :)

# COMMAND ----------

