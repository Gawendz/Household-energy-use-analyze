from etl import create_spark, load_raw, clean_df

def main():
    spark = create_spark("HouseholdEnergy_Clean")
    df_raw   = load_raw(spark)
    df_clean = clean_df(df_raw)

    out_path = "data/cleaned_parquet"
    df_clean.write.mode("overwrite").parquet(out_path)
    print(f"[Clean] Zapisano {df_clean.count()} wierszy do {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
