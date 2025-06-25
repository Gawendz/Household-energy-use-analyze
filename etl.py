from ucimlrepo import fetch_ucirepo
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    try_to_timestamp,
    concat_ws,
    col
)

# wzorzec do parsowania Date+Time
DATE_PATTERN = "d/M/yyyy H:mm:ss"

def create_spark(app_name="HouseholdEnergyETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def load_raw(spark):
    """
    Pobiera surowe dane z UCI repo (ID=235) i zwraca Spark DataFrame.
    """
    ds = fetch_ucirepo(id=235)
    return spark.createDataFrame(ds.data.features)

def clean_df(df):
    """
    Czyści DataFrame:
      1) '?' → null
      2) rzutuje numeryczne kolumny na double
      3) skleja Date+Time i parsuje timestamp
      4) usuwa wiersze z nieparsowalnym datetime
    """
    # 1) zamiana "?" na NULL
    df = df.replace("?", None)

    # 2) rzutowanie kolumn na double
    num_cols = [
        "Global_active_power",
        "Global_reactive_power",
        "Voltage",
        "Global_intensity",
        "Sub_metering_1",
        "Sub_metering_2",
        "Sub_metering_3"
    ]
    for c in num_cols:
        df = df.withColumn(c, col(c).cast("double"))

    # 3) parsowanie Date+Time
    df = df.withColumn(
        "datetime",
        try_to_timestamp(
            concat_ws(" ", col("Date"), col("Time")),
            lit(DATE_PATTERN)
        )
    )
    bad = df.filter(col("datetime").isNull()).count()
    print(f"[ETL] Unparsable datetime rows: {bad}")
    return df.filter(col("datetime").isNotNull())
