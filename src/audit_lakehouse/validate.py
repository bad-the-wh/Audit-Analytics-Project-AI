"""Compare the audit tests with the generator's answer key: did we find every planted anomaly, and nothing else?"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .jet import RULES


def score_against_answer_key(results: DataFrame, answer_key: DataFrame) -> DataFrame:
    """Per rule: true positives, missed anomalies, false positives, recall and precision."""
    rows = []
    for flag in RULES:
        planted = answer_key.filter(F.col("anomaly_type") == flag).select("entry_id").withColumn("planted", F.lit(True))
        r = (
            results.select("entry_id", F.col(flag).alias("flagged"))
            .join(planted, "entry_id", "full_outer")
            .fillna(False, subset=["flagged", "planted"])
            .agg(
                F.lit(flag).alias("rule"),
                F.sum((F.col("flagged") & F.col("planted")).cast("int")).alias("true_positives"),
                F.sum((~F.col("flagged") & F.col("planted")).cast("int")).alias("missed"),
                F.sum((F.col("flagged") & ~F.col("planted")).cast("int")).alias("false_positives"),
            )
        )
        rows.append(r)
    out = rows[0]
    for r in rows[1:]:
        out = out.unionByName(r)
    tp, fn, fp = F.col("true_positives"), F.col("missed"), F.col("false_positives")
    return (
        out.withColumn("recall_pct", F.round(100 * tp / (tp + fn), 1))
        .withColumn("precision_pct", F.round(100 * tp / F.when((tp + fp) > 0, tp + fp), 1))
    )


def score_quarantine(quarantine: DataFrame, dq_key: DataFrame) -> DataFrame:
    """Every entry with a planted data-quality defect must be in quarantine, and no other entry."""
    q = quarantine.select("entry_id").distinct().withColumn("quarantined", F.lit(True))
    k = dq_key.select("entry_id").distinct().withColumn("defective", F.lit(True))
    return (
        q.join(k, "entry_id", "full_outer").fillna(False, subset=["quarantined", "defective"])
        .agg(
            F.sum((F.col("quarantined") & F.col("defective")).cast("int")).alias("correctly_quarantined"),
            F.sum((~F.col("quarantined") & F.col("defective")).cast("int")).alias("missed"),
            F.sum((F.col("quarantined") & ~F.col("defective")).cast("int")).alias("wrongly_quarantined"),
        )
    )
