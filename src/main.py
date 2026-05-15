def main():

    spark = create_spark_session()

    raw_df = load_raw_data(spark, RAW_PATH)

    cleaned_df = clean_transactions(raw_df)

    enriched_df = enrich_data(cleaned_df)

    write_delta(enriched_df, CURATED_PATH)

if __name__ == "__main__":
    main()