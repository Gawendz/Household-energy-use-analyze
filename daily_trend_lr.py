import math, pandas as pd, matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, col, sum as _sum,
    dayofyear, dayofweek, sin, cos,
    monotonically_increasing_id as m_id,
    isnan, lit
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

PARQUET = "data/cleaned_parquet"

spark = (SparkSession.builder
         .appName("DailyTrendLinearSeasonal")
         .master("local[*]")
         .getOrCreate())

#dzienne kWh
df = (spark.read.parquet(PARQUET)
        .withColumn("date", to_date(col("date")))
        .groupBy("date")
        .agg((_sum("Global_active_power")/60).alias("kwh"))
        .orderBy("date"))

#trend + sezon + dzien
df = (df
      .withColumn("doy", dayofyear("date").cast("double"))
      .withColumn("dow", dayofweek("date")-1)
      .withColumn("sin_doy", sin(2*math.pi*col("doy")/365))
      .withColumn("cos_doy", cos(2*math.pi*col("doy")/365))
      .withColumn("t_idx", m_id()))

for i in range(7):                                
    df = df.withColumn(f"dow_{i}", (col("dow")==lit(i)).cast("double"))

df = df.filter(~isnan("kwh") & col("kwh").isNotNull())


feature_cols = ["t_idx", "sin_doy", "cos_doy"] + [f"dow_{i}" for i in range(7)]
data = (VectorAssembler(inputCols=feature_cols, outputCol="features")
        .transform(df)
        .select("t_idx", "features", col("kwh").alias("label")))   

train, test = data.randomSplit([0.8, 0.2], seed=42)

lr    = LinearRegression(maxIter=50, regParam=0.0)
model = lr.fit(train)

pred  = model.transform(test)
rmse  = RegressionEvaluator(metricName="rmse").evaluate(pred)
r2    = RegressionEvaluator(metricName="r2").evaluate(pred)

print(f"\nLinearRegression (trend + sezon + dni tyg.)")
print(f"RMSE: {rmse:,.2f} kWh   |   R²: {r2:.4f}")

pdf = (pred
       .select(col("t_idx").cast("int").alias("day_idx"),
               col("label").alias("kwh_real"),
               col("prediction").alias("kwh_pred"))
       .orderBy("day_idx")
       .toPandas())

pdf.to_csv("lr_pred_vs_real.csv", index=False)
print("Zapisano lr_pred_vs_real.csv")

last60 = pdf.tail(60)
plt.figure(figsize=(10,4))
plt.plot(last60["day_idx"], last60["kwh_real"], lw=1, label="Rzeczywiste")
plt.plot(last60["day_idx"], last60["kwh_pred"], lw=2, label="Predykcja LR")
plt.title("Linear Regression – ostatnie 60 dni vs. predykcja")
plt.xlabel("Numer dnia od startu zbioru"); plt.ylabel("kWh")
plt.grid(alpha=0.3); plt.legend(); plt.tight_layout()
plt.savefig("lr_trend.png"); plt.close()
print("Zapisano lr_trend_improved.png")

spark.stop()
