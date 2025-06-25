from ucimlrepo import fetch_ucirepo
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    try_to_timestamp,
    concat_ws,
    col,
    to_date,
    sum as _sum
)

def main():
    # 1) Uruchom SparkSession
    spark = SparkSession.builder \
        .appName("HouseholdEnergy") \
        .master("local[*]") \
        .getOrCreate()

    # 2) Pobierz dane z UCI repozytorium
    ds = fetch_ucirepo(id=235)
    X = ds.data.features  # pandas.DataFrame

    # 3) Konwertuj pandas DataFrame na Spark DataFrame
    df = spark.createDataFrame(X)

    # 4) Zastąp "?" przez null i skonwertuj typy
    df = df.replace("?", None)
    df = df.withColumn("Global_active_power", col("Global_active_power").cast("double"))

    # 5) Parsuj datę i godzinę (elastycznie: d/M/yyyy H:mm:ss), używając lit() dla patternu
    df = df.withColumn(
        "datetime",
        try_to_timestamp(
            concat_ws(" ", col("Date"), col("Time")),
            lit("d/M/yyyy H:mm:ss")
        )
    )

    # 6) Pokaż ile wierszy nie udało się sparsować
    nulls = df.filter(col("datetime").isNull()).count()
    print(f"Unparsable datetime rows: {nulls}")

    # 7) Odfiltruj wiersze bez poprawnej daty
    df = df.filter(col("datetime").isNotNull())

    # 8) Agregacja dzienna: suma Global_active_power
    daily = df.groupBy(to_date(col("datetime")).alias("date")) \
              .agg(_sum("Global_active_power").alias("daily_active_power"))

    # 9) Wyświetl pierwsze 5 dni
    daily.orderBy("date").show(5, truncate=False)

    # 10) Zakończ sesję Sparka
    spark.stop()


if __name__ == "__main__":
    main()