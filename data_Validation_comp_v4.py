#!/usr/bin/env python3
"""Bigplay vs DaaS PySpark regression validator, Version 4.

This is an enhancement of the screenshot-based reconstruction, not the
byte-for-byte original. No pandas dependency. Run with spark-submit.

Preserved decision: the automatic key search checks candidate columns in their
original File1 order and *stops as soon as one column is unique and non-null
in both datasets*. This is a pragmatic uniqueness heuristic, not business-PK
inference. Optionally pass --composite to bypass it.

Examples:
  spark-submit data_Validation_comp_v4.py --bigplay_file bigplay.csv \\
      --daas_file daas.csv --username alice
  spark-submit data_Validation_comp_v3.py --config validation_v3_config.json
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from time import perf_counter

from data_validation_dashboard import render_dashboard

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType

LOG = logging.getLogger('DataValidationV4')

# Exact legacy folder names are intentionally kept for users of Version 2.
WIDE_OUTPUT = 'DataValidation_differencesall_records.csv'
MISSING_OUTPUT = 'DataValidation_differencesmissing_records.csv'
COLUMN_OUTPUT = 'DataValidation_differences_by_column'
COLUMN_SUMMARY = 'DataValidation_column_summary.csv'
SCHEMA_OUTPUT = 'DataValidation_schema_differences.csv'
CONVERSION_OUTPUT = 'DataValidation_invalid_conversions.csv'
KEY_SAMPLE_OUTPUT = 'DataValidation_top_affected_keys.csv'
RECON_OUTPUT = 'DataValidation_aggregate_reconciliation.csv'
QUALITY_OUTPUT = 'DataValidation_data_quality.csv'
SUMMARY_FILE = 'DataValidation_summary.json'
DASHBOARD_FILE = 'DataValidation_dashboard.html'

DEFAULTS = {
    'infer_schema': True,  # v2 compatible; set false for 00123/CUSIP preservation
    'null_values': ['NULL', ''],
    'exclude_columns': [],
    'numeric_columns': [],
    'column_types': {},  # optional source-type overrides, e.g. {"CUSIP":"string"}
    'rules': {},  # per-field numeric, tolerance, scale, case, trim, date_format
    'numeric_tolerance': '0',
    'rounding_scale': 4,
    'reconciliations': [],
    'required_columns': [],
    'thresholds': {},
    'spark': {'shuffle_partitions': 16, 'cache_inputs': True,
              'output_partitions': 8, 'single_csv_max_rows': 0},
    'top_keys_limit': 20,
    'dashboard': {'enabled': True, 'sample_rows': 100, 'max_columns': 500},
    'file1_label': 'Bigplay',
    'file2_label': 'DaaS',
    'fail_on_schema_change': False,
    'fail_on_invalid_conversion': True,
}


def qcol(name):
    """Column reference safe for spaces, periods, and backticks."""
    return F.col('`' + name.replace('`', '``') + '`')


def ac(alias, name):
    return F.col('`' + alias.replace('`', '``') + '`.`' + name.replace('`', '``') + '`')


def normalize_name(name):
    return name.strip().replace(' ', '_')


def dec(value, label='numeric value'):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f'Invalid {label}: {value!r}') from exc
    if not number.is_finite() or number < 0:
        raise ValueError(f'{label} must be non-negative and finite: {value!r}')
    return number


def load_config(path):
    if not path:
        return {}
    path = Path(path)
    with path.open(encoding='utf-8') as handle:
        if path.suffix.lower() in ('.yaml', '.yml'):
            try:
                import yaml
            except ImportError as exc:
                raise ValueError('YAML requires PyYAML; use JSON instead.') from exc
            data = yaml.safe_load(handle)
        else:
            data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError('Configuration must be a JSON/YAML object.')
    return data


def configuration(args):
    user = load_config(args.config)
    unknown = set(user) - set(DEFAULTS) - {
        'bigplay_file', 'daas_file', 'username', 'composite',
        'output_dir', 'run_id',
    }
    if unknown:
        raise ValueError('Unrecognized configuration settings: ' + ', '.join(sorted(unknown)))
    cfg = {**DEFAULTS, **user}
    cfg['spark'] = {**DEFAULTS['spark'], **user.get('spark', {})}
    if not isinstance(user.get('dashboard', {}), dict):
        raise ValueError('dashboard settings must be an object.')
    cfg['dashboard'] = {**DEFAULTS['dashboard'], **user.get('dashboard', {})}
    for name in ('bigplay_file', 'daas_file', 'username', 'output_dir', 'run_id'):
        if getattr(args, name) is not None:
            cfg[name] = getattr(args, name)
    if args.composite is not None:
        cfg['composite'] = args.composite
    if args.numeric_tolerance is not None:
        cfg['numeric_tolerance'] = args.numeric_tolerance
    if not cfg.get('bigplay_file') or not cfg.get('daas_file'):
        raise ValueError('Provide both --bigplay_file and --daas_file (or set them in --config).')
    if not cfg.get('output_dir') and not cfg.get('username'):
        raise ValueError('Supply either --output_dir or --username (possibly in the config).')
    if isinstance(cfg.get('username'), list):
        cfg['username'] = cfg['username'][0] if cfg['username'] else None
    if cfg.get('username') and not re.fullmatch(r'[A-Za-z0-9_.-]+', cfg['username']):
        raise ValueError('username must be a simple Unix username.')
    if not isinstance(cfg['rules'], dict) or not isinstance(cfg['thresholds'], dict):
        raise ValueError('rules and thresholds must be JSON objects.')
    if not isinstance(cfg['spark'], dict):
        raise ValueError('spark settings must be a JSON object.')
    if not isinstance(cfg['column_types'], dict):
        raise ValueError('column_types must map field names to Spark SQL datatypes.')
    for k in ('shuffle_partitions', 'output_partitions'):
        if int(cfg['spark'][k]) < 1:
            raise ValueError(f'spark.{k} must be positive.')
    if int(cfg['dashboard']['sample_rows']) < 0 or int(cfg['dashboard']['sample_rows']) > 2000:
        raise ValueError('dashboard.sample_rows must be 0 through 2000.')
    if int(cfg['dashboard']['max_columns']) < 0 or int(cfg['dashboard']['max_columns']) > 10000:
        raise ValueError('dashboard.max_columns must be 0 through 10000.')
    if not isinstance(cfg['dashboard']['enabled'], bool):
        raise ValueError('dashboard.enabled must be a boolean.')
    if not 0 <= int(cfg['rounding_scale']) <= 10:
        raise ValueError('rounding_scale must be 0 through 10.')
    dec(cfg['numeric_tolerance'], 'numeric_tolerance')
    for k, rule in cfg['rules'].items():
        if not isinstance(rule, dict):
            raise ValueError(f'Rule for {k} must be an object.')
        if 'tolerance' in rule:
            dec(rule['tolerance'], f'rules.{k}.tolerance')
        if 'scale' in rule and not 0 <= int(rule['scale']) <= 10:
            raise ValueError(f'rules.{k}.scale must be between 0 and 10.')
    supported_thresholds = {'max_missing_rows', 'max_mismatch_records',
                            'max_mismatch_pct', 'max_schema_differences',
                            'max_invalid_casts', 'max_reconciliation_failures',
                            'max_required_nulls'}
    if set(cfg['thresholds']) - supported_thresholds:
        raise ValueError('Unknown thresholds: ' + str(sorted(set(cfg['thresholds']) - supported_thresholds)))
    for k, v in cfg['thresholds'].items():
        dec(v, f'thresholds.{k}')
    run = cfg.get('run_id') or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', str(run)):
        raise ValueError('run_id must be a simple folder name.')
    base = cfg.get('output_dir') or f'file:///home/{cfg["username"]}/'
    cfg['run_dir'] = base.rstrip('/') + '/DataValidation_v4/' + str(run)
    return cfg


def load_data(spark, uri, config):
    """Normalize headers, NULL markers, and optional explicitly typed columns.

    Returns (DataFrame, [type-conversion problems]).
    """
    df = (spark.read.option('header', 'true')
          .option('inferSchema', str(bool(config['infer_schema'])).lower())
          .csv(uri))
    names = [normalize_name(name) for name in df.columns]
    if not names or len(names) != len(set(names)) or len(names) != len(set(n.casefold() for n in names)):
        raise ValueError(f'Empty/duplicate or case-insensitively ambiguous headers after normalization in {uri}.')
    df = df.toDF(*names)
    string_columns = [f.name for f in df.schema.fields if f.dataType.simpleString() == 'string']
    for col in string_columns:
        for token in config['null_values']:
            # Nullify exactly the configured token, not substrings.
            df = df.withColumn(col, F.when(qcol(col) == F.lit(str(token)),
                                           F.lit(None)).otherwise(qcol(col)))
    # Count conversion errors BEFORE replacing original values with NULL.
    # This preserves visibility into bad input when explicit types are used.
    type_overrides = {normalize_name(k): v for k, v in config['column_types'].items()}
    for name in type_overrides:
        if name not in df.columns:
            raise ValueError(f'column_types references missing input column: {name}')
    cast_errors = []
    if type_overrides:
        exprs = [F.sum(F.when(qcol(name).isNotNull() & qcol(name).cast(dtype).isNull(), 1)
                       .otherwise(0)).alias(f'e{i}')
                 for i, (name, dtype) in enumerate(type_overrides.items())]
        measured = df.agg(*exprs).first()
        for i, name in enumerate(type_overrides):
            bad = int(measured[f'e{i}'] or 0)
            if bad:
                cast_errors.append((name, bad, 'INVALID_EXPLICIT_TYPE'))
    for name, sql_type in type_overrides.items():
        df = df.withColumn(name, qcol(name).cast(sql_type))
    return df, cast_errors


def schema_report(file1, file2, ignored):
    types1 = {x.name: x.dataType.simpleString() for x in file1.schema.fields}
    types2 = {x.name: x.dataType.simpleString() for x in file2.schema.fields}
    rows = []
    for name in sorted(set(types1) | set(types2)):
        if name in ignored:
            continue
        if name not in types1:
            issue = 'MISSING_IN_FILE1'
        elif name not in types2:
            issue = 'MISSING_IN_FILE2'
        elif types1[name] != types2[name]:
            issue = 'TYPE_MISMATCH'
        else:
            continue
        rows.append((name, issue, types1.get(name), types2.get(name)))
    return rows


def read_keys(cfg):
    raw = cfg.get('composite') or []
    if isinstance(raw, str):
        raw = [raw]
    parts = [normalize_name(piece) for item in raw for piece in item.split('+') if piece.strip()]
    if len(parts) != len(set(parts)):
        raise ValueError('Duplicate composite key component names supplied.')
    return parts


def first_unique_key(file1, file2, common, n1, n2):
    """KEEP requested early-exit algorithm; no exhaustive search after success."""
    if n1 == 0 or n2 == 0:
        raise ValueError('Automatic key detection needs two nonempty datasets.')
    for name in common:  # preserves File1 column order
        LOG.info('Checking candidate primary key: %s', name)
        unique1 = file1.agg(F.countDistinct(qcol(name)).alias('n')).first()['n']
        if unique1 != n1:
            continue
        unique2 = file2.agg(F.countDistinct(qcol(name)).alias('n')).first()['n']
        if unique2 == n2:
            LOG.info('Using first unique candidate: %s; stopping key search.', name)
            return [name], name
        LOG.info('Candidate %s not unique/non-null in File2; continuing.', name)
    raise ValueError('No unique non-null column found in both datasets. Use --composite.')


def validate_explicit_keys(df, components, label):
    for name in components:
        if name not in df.columns:
            raise ValueError(f'{label} does not contain key column {name!r}.')
    null_check = None
    for name in components:
        term = qcol(name).isNull()
        null_check = term if null_check is None else null_check | term
    if df.where(null_check).limit(1).count():
        raise ValueError(f'{label}: supplied composite key contains NULL values.')
    if df.groupBy(*[qcol(x) for x in components]).count().where(F.col('count') > 1).limit(1).count():
        raise ValueError(f'{label}: supplied composite key is not unique.')


def key_condition(keys):
    """No delimiter concatenation: composite keys cannot collide."""
    condition = None
    for name in keys:
        # String conversion avoids Spark implicit numerical coercion of IDs.
        piece = ac('a', name).cast('string') == ac('b', name).cast('string')
        condition = piece if condition is None else (condition & piece)
    return condition


def key_expression(alias, keys):
    """Single-key PK; composite PK is a structured JSON identifier."""
    if len(keys) == 1:
        return ac(alias, keys[0]).cast('string')
    return F.to_json(F.struct(*[ac(alias, k).cast('string').alias(k) for k in keys]))


def write_csv(df, destination, partitions=None, small_rows=None):
    if small_rows is not None:
        df = df.coalesce(1)  # summary-scale output only
    elif partitions is not None:
        df = df.repartition(int(partitions))
    df.write.mode('overwrite').option('header', 'true').csv(destination)
    LOG.info('Saved %s', destination)


def csv_from_rows(spark, rows, schema, destination):
    df = spark.createDataFrame(rows, schema)
    write_csv(df, destination, small_rows=True)


def write_text(spark, destination, content):
    """Use Hadoop FileSystem APIs to support local file:, HDFS, and object stores."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(destination)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    fs.mkdirs(path.getParent())
    # OutputStreamWriter sends a Java UTF-8 string and supports Hadoop FS URIs
    # without relying on Py4J's Python bytearray -> Java byte[] conversion.
    stream = fs.create(path, True)
    writer = jvm.java.io.OutputStreamWriter(stream, 'UTF-8')
    try:
        writer.write(content)
        writer.flush()
    finally:
        writer.close()


def parse_rules(cfg, compare, file1, file2):
    """Resolve options without modifying original values written to reports."""
    recon_metrics = {normalize_name(x) for task in cfg['reconciliations']
                     for x in task.get('sum_columns', [])}
    unknown = (set(map(normalize_name, cfg['rules'])) |
               set(map(normalize_name, cfg['numeric_columns'])) | recon_metrics) - set(compare)
    if unknown:
        raise ValueError(f'rules/numeric_columns reference excluded or nonexistent columns: {sorted(unknown)}')
    rules = {}
    explicit_numeric = {normalize_name(x) for x in cfg['numeric_columns']}
    # Reconciliation SUM columns must also undergo safe numeric comparisons.
    for task in cfg['reconciliations']:
        explicit_numeric.update(normalize_name(x) for x in task.get('sum_columns', []))
    for name in compare:
        r = cfg['rules'].get(name, cfg['rules'].get(name.replace('_', ' '), {}))
        inferred_numeric = (isinstance(file1.schema[name].dataType, NumericType) or
                            isinstance(file2.schema[name].dataType, NumericType))
        is_numeric = bool(r.get('numeric', name in explicit_numeric or inferred_numeric))
        is_date = bool(r.get('date_format') or r.get('kind') == 'date')
        if is_numeric and is_date:
            raise ValueError(f'{name}: cannot simultaneously be date and numeric.')
        if 'tolerance' in r and not is_numeric:
            raise ValueError(f'{name}: tolerance was specified for a non-numeric column. Set numeric=true.')
        rules[name] = {
            'numeric': is_numeric,
            'date_format': r.get('date_format', 'yyyy-MM-dd') if is_date else None,
            'tolerance': dec(r.get('tolerance', cfg['numeric_tolerance']), f'{name}.tolerance'),
            'scale': int(r.get('scale', cfg['rounding_scale'])),
            'case_sensitive': bool(r.get('case_sensitive', True)),
            'trim_whitespace': bool(r.get('trim_whitespace', False)),
        }
    return rules


def comparable(raw, rule):
    """Normalize a comparison expression, preserving original report values."""
    if rule['numeric']:
        return F.round(raw.cast('decimal(38,10)'), rule['scale'])
    if rule['date_format']:
        return F.to_date(raw.cast('string'), rule['date_format'])
    value = raw.cast('string')
    if rule['trim_whitespace']:
        value = F.trim(value)
    if not rule['case_sensitive']:
        value = F.lower(value)
    return value


def discrepancy_condition(a, b, rule):
    if not rule['numeric']:
        return ~a.eqNullSafe(b)
    threshold = F.lit(str(rule['tolerance'])).cast('decimal(38,10)')
    return ((a.isNull() & b.isNotNull()) |
            (a.isNotNull() & b.isNull()) |
            (a.isNotNull() & b.isNotNull() & (F.abs(a - b) > threshold)))


def conversion_counts(file1, file2, compare, rules):
    """One aggregate action per file, not one count() per numerical column."""
    conversion_fields = {c: r for c, r in rules.items() if r['numeric'] or r['date_format']}
    rows = []
    for label, df in (('File1', file1), ('File2', file2)):
        if not conversion_fields:
            continue
        measures = [F.sum(F.when(qcol(name).isNotNull() & comparable(qcol(name), rule).isNull(), 1)
                          .otherwise(0)).alias(f'c{n}')
                    for n, (name, rule) in enumerate(conversion_fields.items())]
        result = df.agg(*measures).first()
        for i, name in enumerate(conversion_fields):
            bad = int(result[f'c{i}'] or 0)
            if bad:
                rows.append((label, name, bad, 'INVALID_NUMERIC' if rules[name]['numeric'] else 'INVALID_DATE'))
    return rows


def required_null_counts(file1, file2, required):
    if not required:
        return []
    data = []
    for label, df in (('File1', file1), ('File2', file2)):
        for name in required:
            if name not in df.columns:
                raise ValueError(f'{label}: missing required column {name}.')
        aggregations = [F.sum(F.when(qcol(name).isNull(), 1).otherwise(0)).alias(f'n{i}')
                        for i, name in enumerate(required)]
        row = df.agg(*aggregations).first()
        data.extend([(label, name, int(row[f'n{i}'] or 0), 'REQUIRED_NULL')
                     for i, name in enumerate(required) if row[f'n{i}']])
    return data


def make_difference_frames(file1, file2, keys, compare, rules):
    """Create one join and cache only affected matched-key records.

    Returns (cached mismatching join, combined wide file, long per-column file).
    """
    joined = file1.alias('a').join(file2.alias('b'), key_condition(keys), 'inner')
    conds, normals = {}, {}
    for name in compare:
        l = comparable(ac('a', name), rules[name])
        r = comparable(ac('b', name), rules[name])
        normals[name] = (l, r)
        conds[name] = discrepancy_condition(l, r, rules[name])

    # Use a non-ambiguous internal temporary column name.
    if '__dv3_affected' in file1.columns or '__dv3_affected' in file2.columns:
        raise ValueError('Reserved column __dv3_affected already exists in input data.')
    affected = F.concat_ws(',', *[F.when(conds[name], F.lit(name)) for name in compare])
    base = joined.withColumn('__dv3_affected', affected).where(qcol('__dv3_affected') != '').persist(StorageLevel.MEMORY_AND_DISK)

    # Backward-compatible wide report, paired values for EVERY common column.
    wide = base.select(
        key_expression('a', keys).alias(keys[0] if len(keys) == 1 else 'composite_key'),
        qcol('__dv3_affected').alias('Columns_effected'),
        *[ac('a', c).alias('uat_' + c) for c in compare],
        *[ac('b', c).alias('prod_' + c) for c in compare],
    )

    details = []
    for name in compare:
        r = rules[name]
        a, b = normals[name]
        if r['numeric']:
            difference = b - a
            pct = F.when(a.isNotNull() & (a != 0), (difference / F.abs(a)) * F.lit(100))
        else:
            difference = F.lit(None).cast('decimal(38,10)')
            pct = F.lit(None).cast('double')
        dtype = (F.when((a.isNull() & b.isNotNull()) | (a.isNotNull() & b.isNull()),
                        F.lit('NULL_MISMATCH'))
                 .otherwise(F.lit('NUMERIC_DIFFERENCE' if r['numeric']
                                  else 'DATE_DIFFERENCE' if r['date_format'] else 'TEXT_DIFFERENCE')))
        details.append(F.when(conds[name], F.struct(
            F.lit(name).alias('Column_affected'),
            ac('a', name).cast('string').alias('File1'),
            ac('b', name).cast('string').alias('File2'),
            dtype.alias('Difference_type'),
            difference.cast('decimal(38,10)').alias('Numeric_Difference'),
            F.abs(difference).cast('decimal(38,10)').alias('Absolute_Difference'),
            F.round(pct, 4).cast('double').alias('Percentage_Difference'),
        )))
    long = (base.select(key_expression('a', keys).alias('PK'),
                        F.explode(F.array(*details)).alias('d'))
            .where(F.col('d').isNotNull())
            .select('PK', 'd.Column_affected', 'd.File1', 'd.File2',
                    'd.Difference_type', 'd.Numeric_Difference',
                    'd.Absolute_Difference', 'd.Percentage_Difference'))
    return base, wide, long


def missing_frame(file1, file2, keys, columns):
    """Both directions, safe for composite keys and dissimilar source column order."""
    left_only = (file1.alias('a').join(file2.alias('b'), key_condition(keys), 'left_anti')
                 .select(F.lit('File2').alias('Missing_from'),
                         key_expression('a', keys).alias('PK'),
                         *[ac('a', c).cast('string').alias(c) for c in columns]))
    right_only = (file2.alias('b').join(file1.alias('a'), key_condition(keys), 'left_anti')
                  .select(F.lit('File1').alias('Missing_from'),
                          key_expression('b', keys).alias('PK'),
                          *[ac('b', c).cast('string').alias(c) for c in columns]))
    return left_only.unionByName(right_only)


def aggregate_reconciliation(file1, file2, settings):
    """Group totals in each file; returns a long comparison of selected metrics."""
    output = []
    for i, task in enumerate(settings):
        name = task.get('name', f'reconciliation_{i + 1}')
        groups = [normalize_name(x) for x in task.get('group_by', [])]
        totals = [normalize_name(x) for x in task.get('sum_columns', [])]
        if not totals:
            raise ValueError(f'Reconciliation {name} requires sum_columns.')
        for col in groups + totals:
            if col not in file1.columns or col not in file2.columns:
                raise ValueError(f'Reconciliation {name} refers to missing column: {col}.')
        tol = dec(task.get('tolerance', '0'), f'{name}.tolerance')
        def agg(df):
            return (df.groupBy(*[qcol(g) for g in groups])
                    .agg(F.count(F.lit(1)).alias('__rows_present'),
                         *[F.sum(qcol(c).cast('decimal(38,10)')).alias('sum_' + str(j))
                           for j, c in enumerate(totals)]))
        left, right = agg(file1).alias('a'), agg(file2).alias('b')
        join_cond = None
        for g in groups:
            cond = ac('a', g).eqNullSafe(ac('b', g))
            join_cond = cond if join_cond is None else join_cond & cond
        if not groups:
            # Spark global aggregation returns one row even for an empty input.
            joined = left.crossJoin(right)
            group_str = F.lit('ALL')
        else:
            joined = left.join(right, join_cond, 'full_outer')
            group_str = F.to_json(F.struct(*[F.coalesce(ac('a', g), ac('b', g)).cast('string').alias(g) for g in groups]))
        metric_rows = []
        for j, column in enumerate(totals):
            a, b = ac('a', 'sum_' + str(j)), ac('b', 'sum_' + str(j))
            delta = b - a
            # Check whether a grouping is present, even when SUM is NULL.
            absent = ac('a', '__rows_present').isNull() | ac('b', '__rows_present').isNull()
            failed = absent | discrepancy_condition(a, b, {'numeric': True, 'tolerance': tol})
            metric_rows.append(F.struct(
                F.lit(name).alias('Reconciliation'), group_str.alias('Group'),
                F.lit(column).alias('Metric'), a.alias('File1_total'), b.alias('File2_total'),
                delta.alias('Difference'),
                F.when(failed, F.lit('FAIL')).otherwise(F.lit('PASS')).alias('Status'),
            ))
        output.append(joined.select(F.explode(F.array(*metric_rows)).alias('x')).select('x.*'))
    if not output:
        return None
    result = output[0]
    for frame in output[1:]:
        result = result.unionByName(frame)
    return result


def get_summary_status(result, cfg):
    metrics = {
        'max_missing_rows': result['missing_rows'],
        'max_mismatch_records': result['mismatched_records'],
        'max_mismatch_pct': result['mismatch_pct_of_matched'],
        'max_schema_differences': result['schema_difference_count'],
        'max_invalid_casts': result['invalid_conversions'],
        'max_reconciliation_failures': result['reconciliation_failures'],
        'max_required_nulls': result['required_nulls'],
    }
    failures = []
    for name, ceiling in cfg['thresholds'].items():
        if Decimal(str(metrics[name])) > dec(ceiling, name):
            failures.append(f'{name}: {metrics[name]} exceeds {ceiling}')
    if cfg['fail_on_schema_change'] and result['schema_difference_count']:
        failures.append('Schema differences detected')
    if cfg['fail_on_invalid_conversion'] and result['invalid_conversions']:
        failures.append('Invalid numeric/date conversions detected')
    if failures:
        return 'FAIL', failures

    # An explicitly configured threshold only permits that named metric.
    # A threshold for numeric differences must not silently approve schema
    # changes, missing records, or a failed aggregate reconciliation.
    configured = set(cfg['thresholds'])
    uncovered = []
    metric_thresholds = {
        'missing_rows': {'max_missing_rows'},
        'mismatched_records': {'max_mismatch_records', 'max_mismatch_pct'},
        'schema_difference_count': {'max_schema_differences'},
        'reconciliation_failures': {'max_reconciliation_failures'},
        'required_nulls': {'max_required_nulls'},
        'invalid_conversions': {'max_invalid_casts'},
    }
    for metric, limits in metric_thresholds.items():
        if result[metric] and not (limits & configured):
            uncovered.append(metric)
    if uncovered:
        return 'REVIEW', [f'Unapproved differences in: {", ".join(uncovered)}.']
    if any(result[m] for m in metric_thresholds):
        return 'PASS', ['All detected differences are within explicitly configured thresholds.']
    return 'PASS', []


def run(spark, cfg):
    started = perf_counter()
    last = started
    timings = {}

    def mark(name):
        nonlocal last
        now = perf_counter()
        timings[name] = round(now - last, 4)
        last = now

    # Treat invalid numeric/date casts as NULL, then capture their original
    # non-NULL values in conversion_counts(). Spark 4 defaults ANSI on;
    # turning it off avoids aborting the entire job before error reporting.
    spark.conf.set('spark.sql.ansi.enabled', 'false')
    root = cfg['run_dir']
    log_context = {'File1_label': cfg['file1_label'], 'File2_label': cfg['file2_label']}
    f1, explicit_err1 = load_data(spark, cfg['bigplay_file'], cfg)
    f2, explicit_err2 = load_data(spark, cfg['daas_file'], cfg)
    mark('load_csv_and_normalize')
    ignored = set(map(normalize_name, cfg['exclude_columns']))
    schema_rows = schema_report(f1, f2, ignored)
    all_common = [c for c in f1.columns if c in f2.columns]
    if not all_common:
        raise ValueError('Input files have no columns in common.')
    components = read_keys(cfg)
    if components and any(x in ignored for x in components):
        raise ValueError('Cannot exclude a primary-key column.')
    compare = [c for c in all_common if c not in ignored and c not in components]
    if not compare:
        raise ValueError('No comparable non-key columns remain.')
    # Key detection operates on source values before comparison normalization.
    if cfg['spark']['cache_inputs']:
        f1.persist(StorageLevel.MEMORY_AND_DISK)
        f2.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        n1, n2 = f1.count(), f2.count()
        if components:
            validate_explicit_keys(f1, components, 'File1')
            validate_explicit_keys(f2, components, 'File2')
            keys, key_label = components, '+'.join(components)
        else:
            candidates = [c for c in all_common if c not in ignored]
            keys, key_label = first_unique_key(f1, f2, candidates, n1, n2)
            compare = [c for c in compare if c not in keys]
        if not compare:
            raise ValueError('No non-key fields remain to compare.')
        LOG.info('Comparison key: %s; compared columns: %s', key_label, len(compare))
        mark('row_counts_and_key_detection')
        rules = parse_rules(cfg, compare, f1, f2)
        conversion_rows = ([('File1', *r) for r in explicit_err1] +
                           [('File2', *r) for r in explicit_err2] +
                           conversion_counts(f1, f2, compare, rules))
        required = [normalize_name(x) for x in cfg['required_columns']]
        quality_rows = required_null_counts(f1, f2, required)
        mark('comparison_rules_and_quality')

        csv_from_rows(spark, schema_rows, 'Column string,Issue string,File1_type string,File2_type string',
                      root + '/' + SCHEMA_OUTPUT)
        csv_from_rows(spark, conversion_rows, 'File string,Column string,Invalid_rows long,Issue string',
                      root + '/' + CONVERSION_OUTPUT)
        csv_from_rows(spark, quality_rows, 'File string,Column string,Null_rows long,Issue string',
                      root + '/' + QUALITY_OUTPUT)
        mark('write_schema_and_quality_reports')

        # Align shared fields and no others; schema-only differences are separately reported.
        shared = [c for c in all_common if c not in ignored]
        a = f1.select(*[qcol(c) for c in shared])
        b = f2.select(*[qcol(c) for c in shared])
        base, wide, detail = make_difference_frames(a, b, keys, compare, rules)
        try:
            # Materialize the cached discrepancy subset for reusable reports.
            mismatch_n = base.count()
            mark('join_and_identify_discrepancies')

            # Calculate matched-key denominator BEFORE per-column statistics.
            missing = missing_frame(a, b, keys, shared)
            missing.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                missing_counts = {r['Missing_from']: r['count'] for r in
                                  missing.groupBy('Missing_from').count().collect()}
                missing_n = sum(missing_counts.values())
                single_limit = int(cfg['spark']['single_csv_max_rows'])
                write_csv(missing, root + '/' + MISSING_OUTPUT,
                          small_rows=missing_n if single_limit > 0 and missing_n <= single_limit else
                          1 if missing_n == 0 else None)
            finally:
                missing.unpersist()
            mark('missing_key_reports')
            matched = n1 - int(missing_counts.get('File2', 0))
            top_limit = int(cfg['top_keys_limit'])
            column_stats = []
            dashboard_sample = []
            if mismatch_n:
                # Main report is compact only under a user-specified safe bound.
                single_limit = int(cfg['spark']['single_csv_max_rows'])
                wide_small = single_limit > 0 and mismatch_n <= single_limit
                write_csv(wide, root + '/' + WIDE_OUTPUT,
                          small_rows=mismatch_n if wide_small else None)
                # Preserve four original fields FIRST, enrich thereafter.
                per_col_partitions = int(cfg['spark']['output_partitions'])
                (detail.withColumn('For_column', F.col('Column_affected'))
                 .repartition(per_col_partitions)
                 .write.mode('overwrite').option('header', 'true')
                 .partitionBy('For_column').csv(root + '/' + COLUMN_OUTPUT))
                counts = (detail.groupBy('Column_affected')
                          .agg(F.count('*').alias('Mismatch_rows'),
                               F.sum(F.when(F.col('Difference_type') == 'NULL_MISMATCH', 1).otherwise(0)).alias('Null_mismatch_rows'),
                               F.max('Absolute_Difference').alias('Max_absolute_difference'))
                          .withColumn('Mismatch_pct_of_matched',
                                      F.round(F.col('Mismatch_rows') * F.lit(100.0 / matched), 4)
                                      if matched else F.lit(None).cast('double')))
                column_stats = [dict(row.asDict()) for row in counts.collect()]
                # Intentionally bounded: never collect all mismatches on the driver.
                # Spark limit is a preview, not a representative statistical sample.
                if cfg['dashboard']['enabled'] and int(cfg['dashboard']['sample_rows']) > 0:
                    dashboard_sample = [row.asDict() for row in
                                        detail.limit(int(cfg['dashboard']['sample_rows'])).collect()]
                write_csv(counts.orderBy(F.desc('Mismatch_rows')), root + '/' + COLUMN_SUMMARY,
                          small_rows=1)
                if top_limit > 0:
                    most_affected = (detail.groupBy('PK')
                                     .agg(F.count('*').alias('Mismatching_columns'))
                                     .orderBy(F.desc('Mismatching_columns'), 'PK').limit(top_limit))
                    write_csv(most_affected, root + '/' + KEY_SAMPLE_OUTPUT, small_rows=1)
            else:
                LOG.info('No value-level differences found.')
                # Explicitly create an empty combined report, useful for automation.
                write_csv(wide, root + '/' + WIDE_OUTPUT, small_rows=1)
                csv_from_rows(spark, [],
                              'Column_affected string,Mismatch_rows long,Null_mismatch_rows long,Max_absolute_difference decimal(38,10),Mismatch_pct_of_matched double',
                              root + '/' + COLUMN_SUMMARY)
            mark('difference_reports_and_preview')

            recon = aggregate_reconciliation(f1, f2, cfg['reconciliations'])
            recon_fail = 0
            recon_total = 0
            if recon is not None:
                recon.persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    recon_metrics = {r['Status']: r['count'] for r in recon.groupBy('Status').count().collect()}
                    recon_fail = int(recon_metrics.get('FAIL', 0))
                    recon_total = sum(recon_metrics.values())
                    write_csv(recon, root + '/' + RECON_OUTPUT)
                finally:
                    recon.unpersist()
            mark('aggregate_reconciliations')

            result = {
                'version': '4.0', 'run_dir': root, 'inputs': {
                    'File1': cfg['bigplay_file'], 'File2': cfg['daas_file'], **log_context,
                }, 'primary_key': key_label, 'primary_key_columns': keys,
                'pk_strategy': 'first non-null unique column in both files; early exit' if not components else 'explicit composite',
                'file1_rows': n1, 'file2_rows': n2, 'matched_keys': matched,
                'mismatched_records': mismatch_n,
                'mismatch_pct_of_matched': round(mismatch_n * 100.0 / matched, 6) if matched else 0.0,
                'missing_rows': missing_n,
                'missing_from_file1': int(missing_counts.get('File1', 0)),
                'missing_from_file2': int(missing_counts.get('File2', 0)),
                'schema_difference_count': len(schema_rows),
                'schema_differences': [dict(zip(('Column', 'Issue', 'File1_type', 'File2_type'), row)) for row in schema_rows],
                'invalid_conversions': sum(row[2] for row in conversion_rows),
                'required_nulls': sum(row[2] for row in quality_rows),
                'reconciliation_groups': recon_total,
                'reconciliation_failures': recon_fail,
                'columns_compared': compare, 'column_mismatch_summary': column_stats,
                'excluded_columns': sorted(ignored),
                'numeric_columns': [k for k, v in rules.items() if v['numeric']],
                'thresholds': cfg['thresholds'],
            }
            result['status'], result['status_reasons'] = get_summary_status(result, cfg)
            mark('summarize_results')
            result['dashboard_sample_limit'] = int(cfg['dashboard']['sample_rows']) if cfg['dashboard']['enabled'] else 0
            result['dashboard_generated'] = bool(cfg['dashboard']['enabled'])
            result['timings_seconds'] = dict(timings)
            if cfg['dashboard']['enabled']:
                dashboard = render_dashboard(result, dashboard_sample,
                                             max_columns=int(cfg['dashboard']['max_columns']))
                write_text(spark, root + '/' + DASHBOARD_FILE, dashboard)
                mark('write_html_dashboard')
            result['timings_seconds'] = dict(timings)
            result['timings_seconds']['total_before_summary_write'] = round(perf_counter() - started, 4)
            write_text(spark, root + '/' + SUMMARY_FILE, json.dumps(result, indent=2, default=str) + '\n')
            LOG.info('Run completed: %s | file1=%s file2=%s matched=%s mismatched=%s missing=%s',
                     result['status'], n1, n2, matched, mismatch_n, missing_n)
            return result
        finally:
            base.unpersist()
    finally:
        if cfg['spark']['cache_inputs']:
            f1.unpersist()
            f2.unpersist()


def main(argv=None):
    parser = argparse.ArgumentParser(description='PySpark Bigplay vs DaaS data regression validation (v4).')
    parser.add_argument('--config', help='JSON settings file; YAML if PyYAML is installed')
    parser.add_argument('--bigplay_file', help='File1 path (legacy interface)')
    parser.add_argument('--daas_file', help='File2 path (legacy interface)')
    parser.add_argument('--username', nargs='+', help='Legacy Hadoop username; first value used')
    parser.add_argument('--composite', nargs='+', help='Explicit key columns, separated by + or spaces')
    parser.add_argument('--output_dir', help='Base folder URI for results')
    parser.add_argument('--run_id', help='Unique output folder for this run')
    parser.add_argument('--numeric_tolerance', help='Default allowable absolute difference')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        cfg = configuration(args)
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    # Do not force master=local: use spark-submit --master / cluster defaults.
    spark = (SparkSession.builder.appName('DataValidationV4')
             .config('spark.sql.shuffle.partitions', str(cfg['spark']['shuffle_partitions']))
             .getOrCreate())
    try:
        result = run(spark, cfg)
        print(json.dumps({k: result[k] for k in ('status', 'run_dir', 'primary_key',
                                               'mismatched_records', 'missing_rows',
                                               'reconciliation_failures')}, indent=2))
        return 2 if result['status'] == 'FAIL' else 0
    except Exception:
        LOG.exception('Validation failed; check errors and source input.')
        return 3
    finally:
        spark.stop()


if __name__ == '__main__':
    sys.exit(main())
