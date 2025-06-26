import re
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, sum as _sum

# metryka: (kolumna w DF, etykieta w legendzie, tryb agregacji)
METRICS = [
    ("Global_active_power",   "Aktywna [kW]",    "avg"),
    ("Global_reactive_power", "Bierna [kVAR]",   "avg"),
    ("Sub_metering_1",        "Sub1 [Wh]",       "sum"),
    ("Sub_metering_2",        "Sub2 [Wh]",       "sum"),
    ("Sub_metering_3",        "Sub3 [Wh]",       "sum"),
    ("Voltage",               "Napięcie [V]",    "avg"),
    ("Global_intensity",      "Natężenie [A]",   "avg"),
]

def slugify(label: str) -> str:
    return re.sub(r'\W+', '_', label.lower()).strip('_')

def create_spark():
    return SparkSession.builder \
        .appName("HourlyCombinedPattern") \
        .master("local[*]") \
        .getOrCreate()

def load_hourly_df(spark):
    df = (spark.read.parquet("data/cleaned_parquet")
          .withColumn("hour", hour("datetime")))
    data = {}
    for col_name, label, mode in METRICS:
        agg = (df.groupBy("hour")
                 .agg((avg(col_name) if mode=="avg" else _sum(col_name))
                      .alias("val")))
        pdf = agg.toPandas().sort_values("hour").set_index("hour")
        data[label] = pdf["val"]
    return pd.DataFrame(data).fillna(0)

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df_n = pd.DataFrame(index=df.index)
    for c in df.columns:
        mn, mx = df[c].min(), df[c].max()
        print(f"{c}: min={mn}, max={mx}")
        if mx > mn:
            df_n[c] = (df[c] - mn) / (mx - mn)
        else:
            df_n[c] = 0.0
    return df_n

def main():
    spark     = create_spark()
    hourly_df = load_hourly_df(spark)
    spark.stop()

    print("Raw hourly data:")
    print(hourly_df.describe())

    hourly_norm = normalize(hourly_df)

    plt.figure(figsize=(12,6))
    for label in hourly_norm.columns:
        plt.plot(hourly_norm.index, hourly_norm[label],
                 marker="o", label=label)

    plt.title("Znormalizowane profile godzinowe wszystkich metryk")
    plt.xlabel("Godzina dnia")
    plt.ylabel("Znormalizowana wartość (0–1)")
    plt.xticks(range(0,24))
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend(loc="center left", bbox_to_anchor=(1,0.5))
    plt.tight_layout()
    out = "hourly_combined_normalized.png"
    plt.savefig(out)
    plt.close()
    print(f"[Viz] Zapisano {out}")

    # 2) osobne normalizowane
    for label in hourly_norm.columns:
        slug = slugify(label)
        plt.figure(figsize=(10,4))
        plt.plot(hourly_norm.index, hourly_norm[label], marker="o")
        plt.title(f"Znormalizowany profil godzinowy: {label}")
        plt.xlabel("Godzina dnia")
        plt.ylabel("Znormalizowana wartość (0–1)")
        plt.xticks(range(0,24))
        plt.grid(True, linestyle="--", alpha=0.5)
        plt.tight_layout()
        fname = f"hourly_{slug}_normalized.png"
        plt.savefig(fname)
        plt.close()
        print(f"[Viz] Zapisano {fname}")

if __name__=="__main__":
    main()
