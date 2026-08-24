import pytest
from pyspark.sql import SparkSession

from src.transformations.enrichment import enrich_transactions


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_enrichment")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_purchase_segment_by_order_amount(spark):
    # Low: 2£, Medium: 10£, High + High_spender: 150£ (> 100)
    raw = spark.createDataFrame(
        [
            (
                "536365",
                "85123",
                "A",
                1,
                "01/12/2010 08:26:00",
                2.0,
                "17850",
                "United Kingdom",
            ),
            (
                "536366",
                "85124",
                "B",
                1,
                "01/12/2010 08:26:00",
                10.0,
                "17851",
                "Germany",
            ),
            (
                "536367",
                "85125",
                "C",
                1,
                "01/12/2010 08:26:00",
                150.0,
                "17852",
                "France",
            ),
        ],
        [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ],
    )
    enriched = enrich_transactions(raw).collect()
    by_invoice = {row["InvoiceNo"]: row for row in enriched}

    assert by_invoice["536365"]["Purchase_segment"] == "Low"
    assert by_invoice["536366"]["Purchase_segment"] == "Medium"
    assert by_invoice["536367"]["Purchase_segment"] == "High"
    assert by_invoice["536367"]["High_spender"] is True
    assert by_invoice["536365"]["High_spender"] is False


def test_continent_mapping_europa(spark):
    raw = spark.createDataFrame(
        [
            ("1", "85123", "X", 1, "01/12/2010 08:26:00", 1.0, "10000", "Germany"),
            ("2", "85124", "X", 1, "01/12/2010 08:26:00", 1.0, "10001", "United Kingdom"),
            ("3", "85125", "X", 1, "01/12/2010 08:26:00", 1.0, "10002", "Japan"),
        ],
        [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ],
    )
    rows = {r["Country"]: r["Continent"] for r in enrich_transactions(raw).collect()}
    assert rows["Germany"] == "Europa"
    assert rows["United Kingdom"] == "Europa"
    assert rows["Japan"] == "Asia"
