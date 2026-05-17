import pytest
from pyspark.sql import SparkSession

from src.quality.checks import DataQualityError, run_checks
from src.transformations.cleaning import clean_transactions


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_quality")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_run_checks_passes_after_cleaning(spark):
    raw = spark.createDataFrame(
        [
            ("536365", "85123A", "WHITE HEART", 6, "01/12/2010 08:26:00", 2.55, "17850", "UK"),
            ("536366", "71053", "LANTERN", 6, "01/12/2010 08:28:00", 3.39, "17850", "UK"),
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
    cleaned = clean_transactions(raw)
    report = run_checks(cleaned, raise_on_failure=False)

    assert report["passed"] is True
    assert all(c["violations"] == 0 for c in report["constraints"])


def test_run_checks_raises_on_cancelled_invoice(spark):
    raw = spark.createDataFrame(
        [
            ("C536365", "85123A", "CANCELLED", 1, "01/12/2010 08:26:00", 2.55, "17850", "UK"),
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
    with pytest.raises(DataQualityError) as exc_info:
        run_checks(raw, raise_on_failure=True)

    assert exc_info.value.report["passed"] is False
    failed = {c["name"] for c in exc_info.value.report["constraints"] if not c["passed"]}
    assert "invoice_not_cancelled" in failed
