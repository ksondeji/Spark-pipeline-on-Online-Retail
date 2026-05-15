def write_delta(df, path, mode="overwrite", partition_cols=None):

    writer = (
        df.write
        .format("delta")
        .mode(mode)
    )

    if partition_cols:
        writer = writer.partitionBy(partition_cols)

    writer.save(path)