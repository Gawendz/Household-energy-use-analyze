import re
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, sum as _sum, when, col, lit

PARQUET = "data/cleaned_parquet"

# kolumna, etykieta, tryb agregacji, dzielnik (np. 60 → kWh)
METRICS = [
    ("Global_active_power",   "Aktywna [kWh]",     "sum", 60),
    ("Global_reactive_power", "Bierna [kVARh]",    "sum", 60),
    ("Sub_metering_1",        "Sub1 [Wh]",         "sum", 1),
    ("Sub_metering_2",        "Sub2 [Wh]",         "sum", 1),
    ("Sub_metering_3",        "Sub3 [Wh]",         "sum", 1),
    ("Voltage",               "Napięcie [V]",      "avg", 1),
    ("Global_intensity",      "Natężenie [A]",     "avg", 1),
]

def slugify(text: str) -> str:
    return re.sub(r'\W+', '_', text.lower()).strip('_')

def spark_session():
    return (SparkSession.builder
            .appName("HourlyProfiles")
            .master("local[*]")
            .getOrCreate())

def load_hourly_pdf(spark) -> pd.DataFrame:
    df = spark.read.parquet(PARQUET)

    for col_name, *_ in METRICS:
        df = (df.withColumn(col_name, col(col_name).cast("double"))
                .withColumn(col_name, when(col(col_name).isNaN(), 0.0)
                                          .otherwise(col(col_name))))

    df = df.withColumn("hour", hour("datetime"))

    data = {}
    for col_name, label, mode, div in METRICS:
        expr = avg(col_name) if mode == "avg" else _sum(col_name) / lit(div)
        agg = (df.groupBy("hour").agg(expr.alias("val"))
                 .orderBy("hour"))
        pdf = agg.toPandas().set_index("hour")["val"]
        data[label] = pdf
    return pd.DataFrame(data)

def save_line(df, cols, title, ylab, fname):
    plt.figure(figsize=(12,6))
    for c in cols:
        plt.plot(df.index, df[c], marker="o", label=c)
    plt.title(title)
    plt.xlabel("Godzina dnia")
    plt.ylabel(ylab)
    plt.xticks(range(0,24))
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend(loc="center left", bbox_to_anchor=(1,0.5))
    plt.tight_layout()
    plt.savefig(fname)
    plt.close()
    print(f"[Viz] Zapisano {fname}")

def normalize(df):
    return (df - df.min()) / (df.max() - df.min())

if __name__ == "__main__":
    spark = spark_session()
    hourly_raw = load_hourly_pdf(spark)
    spark.stop()

    save_line(hourly_raw,
              hourly_raw.columns,
              "Profile godzinowe (jednostki oryginalne)",
              "Wartość",              
              "hourly_combined_raw.png")

    #znormalizowane 0-1
    hourly_norm = normalize(hourly_raw)
    save_line(hourly_norm,
              hourly_norm.columns,
              "Znormalizowane profile godzinowe (0–1)",
              "Znormalizowana wartość",
              "hourly_combined_normalized.png")

    for col in hourly_norm.columns:
        slug = slugify(col)
        save_line(hourly_norm[[col]],
                  [col],
                  f"Znormalizowany profil godzinowy – {col}",
                  "Znormalizowana wartość",
                  f"hourly_{slug}_normalized.png")
