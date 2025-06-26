import pandas as pd, matplotlib.pyplot as plt, seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, sum as _sum

PARQUET = "data/cleaned_parquet"

spark = (SparkSession.builder
         .appName("CalendarHeatMap")
         .master("local[*]")
         .getOrCreate())

# 1) dzienne kWh
daily = (spark.read.parquet(PARQUET)
           .withColumn("date", to_date("date"))
           .groupBy("date")
           .agg((_sum("Global_active_power")/60).alias("kwh")))

# 2) miesięczna suma
monthly = (daily
           .withColumn("yr", year("date"))
           .withColumn("mo", month("date"))
           .groupBy("yr", "mo")
           .agg(_sum("kwh").alias("kwh_month"))
           .toPandas())

# 3) pivot rok×miesiąc
heat = monthly.pivot(index="yr", columns="mo", values="kwh_month") \
              .sort_index()

# 4) rysuj
plt.figure(figsize=(11, 5))
sns.heatmap(heat, cmap="YlOrRd", linewidths=.3, linecolor="gray",
            cbar_kws={"label":"kWh / miesiąc"})
plt.title("Miesięczne zużycie energii")
plt.xlabel("Miesiąc"); plt.ylabel("Rok")
plt.tight_layout()
plt.savefig("calendar_monthly_kwh.png")
plt.close()
print("✅  Zapisano calendar_monthly_kwh.png")

spark.stop()
