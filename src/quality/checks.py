def check_negative_prices(df):

    return df.filter("UnitPrice < 0").count() == 0