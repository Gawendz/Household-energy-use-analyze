from ucimlrepo import fetch_ucirepo
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, try_to_timestamp,
    to_date, when, lit
)

DEST = "data/cleaned_parquet"

def main():
    spark = (SparkSession.builder
             .appName("CleaningFixed")
             .master("local[*]")
             .getOrCreate())

    #dataset jako pandas, „?” → None
    pdf = (fetch_ucirepo(id=235).data.features
           .replace({"?": None, "": None}))

    # pandas → spark
    df = spark.createDataFrame(pdf)

    #rzutowanie kolumn liczbowych +  NaN → null
    num = ["Global_active_power","Global_reactive_power",
           "Voltage","Global_intensity",
           "Sub_metering_1","Sub_metering_2","Sub_metering_3"]
    for c in num:
        df = (df.withColumn(c, col(c).cast("double"))
                .withColumn(c, when(col(c).isNaN(), None).otherwise(col(c))))

    #pełny timestamp i kolumna DATE
    df = (df
          .withColumn("datetime",
              try_to_timestamp(concat_ws(" ", col("Date"), col("Time")),
                               lit("d/M/yyyy H:mm:ss")))
          .withColumn("date", to_date(col("Date"), "d/M/yyyy")))

    #filtracja wierszy bez poprawnej daty
    bad = df.filter(col("date").isNull()).count()
    print(f"[clean] wiersze bez daty: {bad}")
    df = df.filter(col("date").isNotNull())

    #zapis Parquet
    (df.repartition(1)
       .write.mode("overwrite").parquet(DEST))
    print(f"[clean] zapisano {DEST}")

    spark.stop()

if __name__ == "__main__":
    main()
