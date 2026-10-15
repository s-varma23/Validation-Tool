#!/usr/bin/env python3
"""Repeatable Spark benchmark for Data Validation V4.

Generates Spark CSVs, then measures validation wall time separately from data
preparation. Works locally or on a cluster with shared work_dir configured.
"""
import argparse
import csv
import json
import logging
import statistics
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession, functions as F

import data_Validation_comp_v4 as tool


def sizes(value):
    try:
        n = int(value.replace('_', ''))
    except ValueError as exc:
        raise argparse.ArgumentTypeError('Sizes must be positive integers') from exc
    if n < 1:
        raise argparse.ArgumentTypeError('Sizes must be positive')
    return n


def make_frames(spark, rows, extra_columns=10, partitions=4):
    """Deterministic datasets with a leading unique key and controlled differences.

    For rows >= 10, ~10% of source keys are missing from the other side,
    ~1% have a material numeric mismatch, ~0.5% have a text mismatch.
    """
    if rows < 1 or extra_columns < 1:
        raise ValueError('rows and extra_columns must be positive')
    ids = spark.range(rows, numPartitions=partitions)
    fields1 = [(F.col('id') + 1).cast('string').alias('ID')]
    changed_key = F.when(F.col('id') % 10 == 0, F.col('id') + rows + 1).otherwise(F.col('id') + 1)
    fields2 = [changed_key.cast('string').alias('ID')]
    for j in range(extra_columns):
        col = f'COL_{j:03d}'
        if j == 0:
            left = F.format_string('%.2f', (F.col('id') * F.lit(1.23)).cast('double'))
            right = F.format_string('%.2f', ((F.col('id') * F.lit(1.23)) +
                                     F.when(F.col('id') % 101 == 7, F.lit(5.0)).otherwise(F.lit(0.0))).cast('double'))
        elif j == 1:
            left = F.when(F.col('id') % 2 == 0, 'Active').otherwise('Pending')
            right = F.when(F.col('id') % 211 == 5, 'Closed').otherwise(left)
        else:
            left = F.concat(F.lit(f'X{j}-'), (F.col('id') % 53).cast('string'))
            right = left
        # Use original range id for each, NOT the transformed ID, so discrepancies
        # arise only from defined change rules (plus predictable missing keys).
        fields1.append(left.alias(col))
        fields2.append(right.alias(col))
    return ids.select(*fields1), ids.select(*fields2)


def summarize(rows):
    groups = {}
    for row in rows:
        key = (row['records'], row['columns'])
        groups.setdefault(key, []).append(row)
    result = []
    for (n, width), records in sorted(groups.items()):
        elapsed = [r['validation_seconds'] for r in records]
        result.append({'records': n, 'columns': width, 'repetitions': len(records),
                       'median_seconds': round(statistics.median(elapsed), 4),
                       'minimum_seconds': round(min(elapsed), 4),
                       'maximum_seconds': round(max(elapsed), 4),
                       'median_rows_per_second': round(n / statistics.median(elapsed), 2) if min(elapsed) > 0 else None})
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description='Generate realistic Spark CSVs and time DataValidation V4')
    parser.add_argument('--sizes', nargs='+', type=sizes, default=[10000, 100000], help='Row counts per input file')
    parser.add_argument('--columns', nargs='+', type=sizes, default=[10, 50], help='Comparable columns (ID is extra)')
    parser.add_argument('--repetitions', type=int, default=2)
    parser.add_argument('--warmups', type=int, default=0)
    parser.add_argument('--partitions', type=int, default=4)
    parser.add_argument('--master', help='Optional master; defaults to spark-submit / cluster setting')
    parser.add_argument('--work-dir', required=True, help='A filesystem URI accessible to Spark executors')
    parser.add_argument('--metrics-dir', default='benchmark_results', help='Local directory for benchmark metrics')
    parser.add_argument('--no-dashboard', action='store_true', help='Measure validation without HTML dashboard')
    args = parser.parse_args(argv)
    if min(args.repetitions, args.partitions) < 1 or args.warmups < 0:
        parser.error('repetitions/partitions must be positive and warmups cannot be negative')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    builder = SparkSession.builder.appName('DataValidationV4_Benchmark')
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    started_at = datetime.now(timezone.utc).isoformat()
    metrics=[]
    try:
        for n in args.sizes:
            for w in args.columns:
                logging.info('Preparing datasets with %,d rows and %d compare columns', n, w)
                gen_started = perf_counter()
                file1,file2 = make_frames(spark, n, w, args.partitions)
                root = args.work_dir.rstrip('/') + f'/bench_input_n{n}_w{w}'
                file1.write.mode('overwrite').option('header','true').csv(root + '/file1')
                file2.write.mode('overwrite').option('header','true').csv(root + '/file2')
                generation_seconds = round(perf_counter()-gen_started, 4)
                for trial in range(args.warmups+args.repetitions):
                    warmup = trial < args.warmups
                    benchmark_id = f'bench_n{n}_w{w}_{"warmup" if warmup else "trial"}_{trial}'
                    cfg={**tool.DEFAULTS,
                         'bigplay_file':root+'/file1', 'daas_file':root+'/file2',
                         'output_dir':args.work_dir.rstrip('/')+'/benchmark_outputs',
                         'run_id':benchmark_id,
                         'infer_schema': False,
                         'numeric_columns':['COL_000'],
                         'numeric_tolerance':'0.01',
                         'dashboard':{'enabled':not args.no_dashboard,'sample_rows':25,'max_columns':250},
                         'spark': {'shuffle_partitions':max(4,args.partitions*2),
                                   'output_partitions':args.partitions,
                                   'single_csv_max_rows':0,'cache_inputs':True}}
                    # Config gets merged/validated by the normal public interface.
                    from argparse import Namespace
                    cli = Namespace(config=None,bigplay_file=cfg['bigplay_file'],daas_file=cfg['daas_file'],
                                    username=None,output_dir=cfg['output_dir'],composite=None,
                                    run_id=cfg['run_id'],numeric_tolerance=cfg['numeric_tolerance'])
                    configured=tool.configuration(cli)
                    configured.update({k:v for k,v in cfg.items() if k not in ('run_id','output_dir','bigplay_file','daas_file')})
                    t0=perf_counter()
                    result=tool.run(spark,configured)
                    duration=round(perf_counter()-t0,4)
                    if not warmup:
                        metrics.append({'records':n,'columns':w,'trial':trial-args.warmups+1,
                                        'generation_seconds':generation_seconds,
                                        'validation_seconds':duration,
                                        'primary_key':result['primary_key'],
                                        'matched_keys':result['matched_keys'],
                                        'mismatched_records':result['mismatched_records'],
                                        'missing_rows':result['missing_rows'],
                                        'status':result['status'],
                                        'stage_seconds':result.get('timings_seconds',{}),
                                        'run_dir':result['run_dir']})
                    logging.info('%s: validation took %.3fs (%s)',benchmark_id,duration,result['status'])
        info={'version':'4.0', 'started_at':started_at,
              'spark_version':spark.version, 'spark_master':spark.sparkContext.master,
              'spark_default_parallelism':spark.sparkContext.defaultParallelism,
              'rows':metrics,'summary':summarize(metrics),
              'interpretation':'Validation timings exclude dataset generation; full CSV and report writes are included. Compare like-for-like configurations.'}
        dest=Path(args.metrics_dir)
        dest.mkdir(parents=True,exist_ok=True)
        (dest/'benchmark_results.json').write_text(json.dumps(info,indent=2,default=str)+'\n',encoding='utf-8')
        with (dest/'benchmark_results.csv').open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=['records','columns','trial','generation_seconds','validation_seconds','primary_key','matched_keys','mismatched_records','missing_rows','status','run_dir'])
            writer.writeheader()
            for entry in metrics:
                writer.writerow({k:entry.get(k) for k in writer.fieldnames})
        print(json.dumps(info['summary'],indent=2))
        print('Benchmark metrics written to',str(dest))
    finally:
        spark.stop()
    return 0


if __name__=='__main__':
    raise SystemExit(main())
