import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, var_samp

PARQUET = "data/cleaned_parquet"

spark = (SparkSession.builder
         .appName("Variance_CV")
         .master("local[*]")
         .getOrCreate())

cols = ["Global_active_power",
        "Global_reactive_power",
        "Voltage",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3"]

agg_exprs = []
for c in cols:
    agg_exprs += [avg(c).alias(f"{c}_mean"),
                  var_samp(c).alias(f"{c}_var")]

row = spark.read.parquet(PARQUET).agg(*agg_exprs).first()

print("Metryka                 |  Średnia | Wariancja |    CV")
print("-----------------------------------------------------------")
for c in cols:
    μ  = row[f"{c}_mean"]
    σ2 = row[f"{c}_var"]
    cv = (math.sqrt(σ2) / μ) if μ != 0 else float("nan")
    print(f"{c:22s} | {μ:8.3f} | {σ2:9.3f} | {cv:6.3f}")

spark.stop()
