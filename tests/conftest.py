import os
import sys

import pytest
from pyspark.sql import SparkSession

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "data_generator"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]").appName("audit-lakehouse-tests")
        .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


LINE_COLS = ["entry_id", "line_no", "posting_date", "created_at", "fiscal_period", "account_id", "description",
             "debit", "credit", "currency", "user_id", "source", "reference"]


@pytest.fixture
def make_lines(spark):
    """Build a raw (all-string) journal-lines DataFrame from short tuples."""
    def _make(rows):
        full = []
        for r in rows:
            d = dict(zip(LINE_COLS, [None] * len(LINE_COLS)))
            d.update({"currency": "EUR", "reference": "", "description": "Test", "source": "MANUAL"})
            d.update(r)
            full.append(tuple(d[c] for c in LINE_COLS))
        from pyspark.sql import functions as F
        return spark.createDataFrame(full, LINE_COLS).withColumn("_ingested_at", F.current_timestamp())
    return _make


@pytest.fixture
def accounts(spark):
    return spark.createDataFrame(
        [("101000", "Cash", "Asset"), ("210000", "Accrued expenses", "Liability"), ("610000", "Rent", "Expense"),
         ("400000", "Sales", "Revenue")],
        ["account_id", "account_name", "account_type"],
    )


@pytest.fixture
def users(spark):
    return spark.createDataFrame(
        [("U008", "Paul", "Finance", "GL accountant", "50000"), ("U001", "Lea", "Accounts Payable", "AP clerk", "25000"),
         ("BATCH01", "System", "System", "Automated", "0")],
        ["user_id", "name", "department", "role", "approval_limit"],
    )


@pytest.fixture
def periods(spark):
    from pyspark.sql import functions as F
    return spark.createDataFrame([("2025-03", "2025-04-07")], ["fiscal_period", "close_date"]).select(
        "fiscal_period", F.to_date("close_date").alias("close_date"))
