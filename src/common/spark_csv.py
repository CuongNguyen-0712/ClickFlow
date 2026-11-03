"""C1 CSV parsing and bounded sample selection for Spark jobs."""
import yaml

from src.common.config import ROOT
from src.common.schemas import SOURCE_COLUMNS, ddl


def read_source_csv(spark, uri):
    contract = yaml.safe_load((ROOT / 'contracts/source_csv.yaml').read_text(encoding='utf-8'))
    return (spark.read.schema(ddl(SOURCE_COLUMNS))
            .option('header', True).option('enforceSchema', False)
            .option('mode', 'FAILFAST')
            .option('timestampFormat', contract['event_time_format'])
            .option('enableDateTimeParsingFallback', False)
            .csv(uri))


def select_sample(spark, uri, *, rows=100000, day='2019-11-01'):
    from pyspark.sql import functions as F

    if rows < 1:
        raise ValueError('Sample rows must be positive')
    columns = [name for name, _ in SOURCE_COLUMNS]
    # Bound candidate rows before the shuffle. Extra rows allow removal of
    # exact source duplicates. Candidate order follows Spark's input partitions.
    scan_rows = max(rows + 1000, rows * 11 // 10)
    return (read_source_csv(spark, uri)
            .filter(F.to_date('event_time') == F.lit(str(day)).cast('date'))
            .limit(scan_rows).dropDuplicates(columns)
            .orderBy(*columns).limit(rows))


def same_rows(left, right, columns):
    """Compare values and multiplicities, independently of Spark row order."""
    left = left.select(*columns)
    right = right.select(*columns)
    return (not left.exceptAll(right).limit(1).count()
            and not right.exceptAll(left).limit(1).count())
