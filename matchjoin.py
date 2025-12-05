from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("match_data").getOrCreate()
df=spark.read.csv(r"E:\DataEnggFiles\Match_Data.csv",header=True, inferSchema=True)
# df.show(5)

#1️⃣ Q: Count matches per season
# df.groupBy("season").count().show()

#2️⃣ Q: List matches won by MI
# df.filter(col("winner")=="Mumbai Indians").show()

#3️⃣ Q: Show win_by_runsd more than 30 runs
# df.filter(col("win_by_runs")>30).show()

#4️⃣ Q: Find the highest matches win by team per season
# df.groupBy("season").count().show()

#5️⃣ Q: Count how many times each team won
# df.groupBy("winner").count().show()

#7️⃣ Q: Find second highest runs (using RANK) by season
# df.groupBy("season").count().alias("total_count")

df.createOrReplaceTempView("match")

#1. Find matches played on the same date (self join).
spark.sql("""
          select m1.id as match1,
          m2.id as match2,
          m1.date 
          from match m1
          join match m2
          on m1.date=m2.date
          and m1.id<=m2.id
          """).show()

#🔹 2. Find matches played in the same venue (self join).
spark.sql("""
          select m1.id as v1, m2.id , m1.venue as v2 from match m1 join match m2 on m1.venue=m2.venue 
          """).show()
