import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import math

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, col,
    sum   as _sum,
    avg   as _avg,
    max   as _max
)

# METRICS = (alias, oryginalna kolumna, tytuł, jednostka, divisor)
METRICS = [
    ("sum_active_power",   "Global_active_power",   "Dzienne zużycie aktywne",    "kWh",   60),
    ("sum_reactive_power", "Global_reactive_power", "Dzienne zużycie bierne",     "kVARh", 60),
    ("sum_sm1",            "Sub_metering_1",        "Sub_metering_1 dziennie",    "Wh",    1),
    ("sum_sm2",            "Sub_metering_2",        "Sub_metering_2 dziennie",    "Wh",    1),
    ("sum_sm3",            "Sub_metering_3",        "Sub_metering_3 dziennie",    "Wh",    1),
    ("avg_voltage",        "Voltage",               "Średnie napięcie dzienne",   "V",     1),
    ("avg_intensity",      "Global_intensity",      "Średnie natężenie dzienne",  "A",     1),
]

def create_spark(app_name="HouseholdEnergyViz"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

def load_cleaned(spark, path="data/cleaned_parquet"):
    return spark.read.parquet(path)

def daily_stats(df):
    df2 = df.withColumn("date", to_date(col("datetime")))
    aggs = []
    for alias, orig, *_ in METRICS:
        if alias.startswith("avg_"):
            aggs.append(_avg(orig).alias(alias))
        else:
            aggs.append(_sum(orig).alias(alias))
    return df2.groupBy("date").agg(*aggs).orderBy("date")

def plot_single(pdf, alias, title, unit, divisor):
    series    = pdf[alias] / divisor
    mean_val  = series.mean()
    peak_val  = series.max()
    peak_date = pdf.loc[series.idxmax(), 'date']

    plt.figure(figsize=(12,6))
    plt.plot(pdf['date'], series, label=title)
    plt.axhline(mean_val, color='grey', linestyle='--',
                label=f"Średnia: {mean_val:.2f} {unit}")
    plt.scatter([peak_date], [peak_val], color='red', zorder=5,
                label=f"Szczyt: {peak_val:.2f} {unit}\n{peak_date.date()}")

    plt.title(f"{title} [{unit}]")
    plt.xlabel("Data")
    plt.ylabel(unit)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    out = f"viz_{alias}.png"
    plt.savefig(out)
    plt.close()
    print(f"[Viz] Zapisano {out}")


def plot_overview(pdf):
    import math
    n    = len(METRICS)
    cols = 2
    rows = math.ceil(n/cols)
    fig, axes = plt.subplots(rows, cols, figsize=(14, rows*3), sharex=True)
    axes = axes.flatten()

    fig.suptitle("Przegląd dziennych metryk zużycia i parametrów", fontsize=16)

    for ax, (alias, _, title, unit, divisor) in zip(axes, METRICS):
        series    = pdf[alias] / divisor
        mean_val  = series.mean()
        peak_idx  = series.idxmax()
        peak_date = pdf.loc[peak_idx, 'date']
        peak_val  = series.max()

        ax.plot(pdf['date'], series, label=title)
        ax.axhline(mean_val, color='grey', linestyle='--',
                   label=f"Średnia: {mean_val:.2f} {unit}")
        ax.scatter(peak_date, peak_val, color='red', s=20,
                   label=f"Szczyt: {peak_val:.2f} {unit}\n{peak_date.date()}")

        ax.set_title(f"{title} [{unit}]", fontsize=12)
        ax.set_xlabel("")  # usuń etykietę osi X
        ax.set_ylabel("")  # usuń etykietę osi Y
        ax.tick_params(axis='x', rotation=30)
        ax.legend(fontsize='small', loc="upper left")

    # Usuń puste osie, jeśli METRICS ma nieparzystą liczbę
    for empty in axes[n:]:
        fig.delaxes(empty)

    plt.tight_layout()
    fig.subplots_adjust(top=0.92)  # odsunięcie od suptitle
    out = "overview_metrics.png"
    fig.savefig(out)
    plt.close()
    print(f"[Viz] Zapisano {out}")




def main():
    spark    = create_spark()
    df_clean = load_cleaned(spark)
    daily    = daily_stats(df_clean)
    pdf      = daily.toPandas()
    spark.stop()

    pdf['date'] = pd.to_datetime(pdf['date'])
    for alias, _, title, unit, divisor in METRICS:
        plot_single(pdf, alias, title, unit, divisor)
    plot_overview(pdf)

if __name__ == "__main__":
    main()
