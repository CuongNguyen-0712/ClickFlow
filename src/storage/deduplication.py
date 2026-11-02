"""Rule 5 runs only on valid events; C5 errors bypass event-time filtering."""


def deduplicate_valid_records(classified, watermark='1 hour'):
    from pyspark.sql import functions as F

    valid = classified.filter(F.col('error_reason').isNull())
    invalid = classified.filter(F.col('error_reason').isNotNull())
    deduplicated = (valid.withWatermark('event_time', watermark)
                   .dropDuplicatesWithinWatermark(['event_id']))
    return deduplicated.unionByName(invalid)
