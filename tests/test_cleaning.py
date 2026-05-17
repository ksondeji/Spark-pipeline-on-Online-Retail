import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from src.quality.checks import run_checks
from src.transformations.cleaning import clean_transactions

COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_cleaning")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_cleaning_transactions(spark):
    raw = spark.createDataFrame(
        [
            # conservée : facture valide, StockCode sur 5 caractères
            ("536365", "85123", "WHITE HEART", 6, "01/12/2010 08:26:00", 2.55, "17850", "United Kingdom"),
            # exclue : annulation (préfixe c)
            ("c536346", "71053", "LANTERN", 2, "01/12/2010 08:28:00", 3.39, "21472", "United Kingdom"),
            # exclue : facture aberrante 541431
            ("541431", "43690", "WHITE MEAL", 3, "01/12/2010 08:26:00", 1.87, "79850", "United Kingdom"),
            # exclue : StockCode != 5 caractères
            ("536366", "71053AD3", "ORANGE PAPER", 7, "01/12/2010 08:28:00", 5.19, "23096", "United Kingdom"),
            # exclue : CustomerID null
            ("536367", "85124", "NO CUSTOMER", 1, "01/12/2010 09:00:00", 1.0, None, "France"),
        ],
        COLUMNS,
    )

    cleaned = clean_transactions(raw)

    assert cleaned.count() == 1

    row = cleaned.collect()[0]
    assert row["InvoiceNo"] == "536365"
    assert row["StockCode"] == "85123"
    assert row["Quantity"] == 6
    assert row["UnitPrice"] == 2.55

    schema = {field.name: field.dataType for field in cleaned.schema.fields}
    assert isinstance(schema["Quantity"], IntegerType)
    assert isinstance(schema["UnitPrice"], DoubleType)
    assert isinstance(schema["InvoiceDate"], TimestampType)

    report = run_checks(cleaned, raise_on_failure=False)
    assert report["passed"] is True
