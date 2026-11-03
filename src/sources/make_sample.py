"""Create a bounded C1 sample from MinIO using Spark; never overwrite."""
import argparse
import csv
import json
from pathlib import Path

from src.common.config import load
from src.common.schemas import SOURCE_COLUMNS
from src.common.spark_csv import read_source_csv, same_rows, select_sample
from src.common.spark_session import get_spark
from src.storage.init_lake import lake_paths


def make_sample(spark, source, output, *, rows=100000, day='2019-11-01'):
    output = Path(output)
    sample = select_sample(spark, source, rows=rows, day=day).cache()
    try:
        actual = sample.count()
        if actual != rows:
            raise ValueError(f'Need {rows} distinct events for {day}; bounded input contains {actual}')
        columns = [name for name, _ in SOURCE_COLUMNS]
        reused = output.exists()
        if reused:
            existing = read_source_csv(spark, str(output))
            if not same_rows(existing, sample, columns):
                raise ValueError(f'Existing sample has different content; choose another output: {output}')
        else:
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open('x', encoding='utf-8', newline='') as stream:
                writer = csv.writer(stream, lineterminator='\n')
                writer.writerow(columns)
                for row in sample.toLocalIterator():
                    writer.writerow([
                        row.event_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
                        *['' if value is None else value for value in tuple(row)[1:]],
                    ])
            if not same_rows(read_source_csv(spark, str(output)), sample, columns):
                raise AssertionError('Written CSV differs from selected source values')
        result = {'event': 'sample_csv_ready', 'source': source, 'path': str(output),
                  'rows': actual, 'day': str(day), 'reused': reused}
        print(json.dumps(result))
        return result
    finally:
        sample.unpersist()


def main():
    config = load()['stream']
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default=lake_paths()['raw_csv'] + '2019-Nov.csv')
    parser.add_argument('--output', default=config['sample_path'])
    parser.add_argument('--rows', type=int, default=config.get('sample_rows', 100000))
    parser.add_argument('--day', default=config.get('sample_day', '2019-11-01'))
    args = parser.parse_args()
    spark = get_spark('make-sample')
    spark.sparkContext.setLogLevel('WARN')
    try:
        make_sample(spark, args.source, args.output, rows=args.rows, day=args.day)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
