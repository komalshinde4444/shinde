from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round
from pyspark.sql.types import DoubleType

# -------------------------------------------------
# 1️⃣ Spark Session Initialization
# -------------------------------------------------
spark = SparkSession.builder \
    .appName("FRS_Data_Engineering_Pipeline") \
    .getOrCreate()

# -------------------------------------------------
# 2️⃣ Data Ingestion (from CSV or Excel exported as CSV)
# -------------------------------------------------
# Note: PySpark can’t read Excel directly, so first save the file as CSV.
input_path = r"C:\Users\utkar\OneDrive\Desktop\powerBI\data\data\model_auth_Rep"

model_auth_Rep = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

print("✅ Data loaded successfully:")
model_auth_Rep.printSchema()
model_auth_Rep.show(5)

# -------------------------------------------------
# 3️⃣ Data Validation & Cleaning
# -------------------------------------------------
# Replace nulls in DaysPastDue with 0
model_auth_Rep = model_auth_Rep.fillna({"DaysPastDue": 0})

# Show null count summary
print("🔍 Null values count per column:")
model_auth_Rep.select(
    [col(c).isNull().cast("int").alias(c) for c in model_auth_Rep.columns]
).groupBy().sum().show()

# -------------------------------------------------
# 4️⃣ ECL Calculations
# -------------------------------------------------
model_auth_Rep = model_auth_Rep \
    .withColumn("stage1ecl", round(col("EAD") * col("PD12") * col("LGD"), 2)) \
    .withColumn("stage2ecl", round(col("EAD") * col("PDLT") * col("LGD"), 2)) \
    .withColumn("stage3ecl", round(col("EAD") * col("LGD"), 2))

ecl_df = model_auth_Rep.select("EAD", "PD12", "PDLT", "LGD", "stage1ecl", "stage2ecl", "stage3ecl")
print("✅ Stage ECL DataFrame:")
ecl_df.show(5)

# -------------------------------------------------
# 5️⃣ EAD Variation Report
# -------------------------------------------------
model_auth_Rep = model_auth_Rep \
    .withColumn("change_EAD", round(col("EAD") - col("Previous EAD"), 2)) \
    .withColumn("per_change_EAD",
                round(((col("EAD") - col("Previous EAD")) / col("Previous EAD")) * 100, 2))

ead_df = model_auth_Rep.select("EAD", "Previous EAD", "change_EAD", "per_change_EAD")
print("✅ EAD Variation Report:")
ead_df.show(5)

# -------------------------------------------------
# 6️⃣ LGD Variation Report
# -------------------------------------------------
model_auth_Rep = model_auth_Rep \
    .withColumn("change_LGD", round(col("LGD") - col("Previous LGD"), 2)) \
    .withColumn("per_change_LGD",
                round(((col("LGD") - col("Previous LGD")) / col("Previous LGD")) * 100, 2))

lgd_df = model_auth_Rep.select("LGD", "Previous LGD", "change_LGD", "per_change_LGD")
print("✅ LGD Variation Report:")
lgd_df.show(5)

# -------------------------------------------------
# 7️⃣ Data Storage (Write to Parquet)
# -------------------------------------------------
output_path = r"C:\Users\utkar\OneDrive\Documents\project"


ecl_df.coalesce(1).write.mode("overwrite").option("header", True).csv(r"E:\FRS\ECL_DF\ecl_df.csv")
ead_df.coalesce(1).write.mode("overwrite").option("header", True).csv(r"E:\FRS\EAD_DF\ead_df.csv")
lgd_df.coalesce(1).write.mode("overwrite").option("header", True).csv(r"E:\FRS\LGD_DF\lgd_df.csv")

print("✅ Data successfully written as csv files!")

# -------------------------------------------------
# 8️⃣ Register Temp Views for SQL Queries
# -------------------------------------------------
ecl_df.createOrReplaceTempView("ecl_table")
ead_df.createOrReplaceTempView("ead_table")
lgd_df.createOrReplaceTempView("lgd_table")

spark.sql("""
    SELECT ROUND(AVG(per_change_LGD), 2) AS avg_LGD_change
    FROM lgd_table
""").show()


spark.sql("select * from ead_table limit 10")
# -------------------------------------------------
# ✅ End of Pipeline
# -------------------------------------------------
spark.stop()


