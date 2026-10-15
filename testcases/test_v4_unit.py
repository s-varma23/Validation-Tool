"""Fast tests; run without installing Spark: python -m unittest discover -s tests -v"""
import argparse
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
HAS_SPARK = importlib.util.find_spec('pyspark') is not None
if not HAS_SPARK:
    pkg = types.ModuleType('pyspark')
    pkg.StorageLevel = types.SimpleNamespace(MEMORY_AND_DISK='test')
    sql = types.ModuleType('pyspark.sql')
    sql.functions = types.ModuleType('pyspark.sql.functions')
    sql.SparkSession = object
    dt = types.ModuleType('pyspark.sql.types')
    dt.NumericType = type('NumericType', (), {})
    sys.modules.update({'pyspark': pkg, 'pyspark.sql': sql,
                        'pyspark.sql.functions': sql.functions, 'pyspark.sql.types': dt})

import data_Validation_comp_v4 as tool
from data_validation_dashboard import render_dashboard


def args(**overrides):
    data=dict(config=None,bigplay_file='one.csv',daas_file='two.csv',username=['analyst'],
              output_dir=None,composite=None,run_id='unit_run',numeric_tolerance=None)
    data.update(overrides)
    return argparse.Namespace(**data)


class ConfigurationTests(unittest.TestCase):
    def test_v4_path_and_dashboard_defaults(self):
        cfg=tool.configuration(args())
        self.assertEqual(cfg['run_dir'],'file:///home/analyst/DataValidation_v4/unit_run')
        self.assertEqual(cfg['dashboard']['sample_rows'],100)
        self.assertEqual(cfg['dashboard']['enabled'],True)

    def test_existing_cli_and_composite_preserved(self):
        cfg=tool.configuration(args(composite=['ACCOUNT_ID+FUND_ID','DATE']))
        self.assertEqual(tool.read_keys(cfg), ['ACCOUNT_ID','FUND_ID','DATE'])
        self.assertEqual(cfg['bigplay_file'],'one.csv')
        with self.assertRaises(ValueError):
            tool.read_keys({'composite':['A+A']})

    def test_dashboard_configuration_bounds(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'config.json'
            p.write_text(json.dumps({'dashboard':{'sample_rows':2001}}))
            with self.assertRaisesRegex(ValueError,'sample_rows'):
                tool.configuration(args(config=str(p)))
            p.write_text(json.dumps({'dashboard':{'sample_rows':0,'enabled':False}}))
            cfg=tool.configuration(args(config=str(p)))
            self.assertFalse(cfg['dashboard']['enabled'])
            self.assertEqual(cfg['dashboard']['sample_rows'],0)

    def test_numeric_tolerance_rejects_negative(self):
        with self.assertRaises(ValueError): tool.configuration(args(numeric_tolerance='-1'))

    def test_parse_status(self):
        cfg={'thresholds':{},'fail_on_schema_change':False,'fail_on_invalid_conversion':True}
        record=dict(missing_rows=0,mismatched_records=0,mismatch_pct_of_matched=0,
                    schema_difference_count=0,invalid_conversions=0,
                    reconciliation_failures=0,required_nulls=0)
        self.assertEqual(tool.get_summary_status(record,cfg)[0],'PASS')
        record['missing_rows']=1
        self.assertEqual(tool.get_summary_status(record,cfg)[0],'REVIEW')
        cfg['thresholds']['max_missing_rows']=1
        self.assertEqual(tool.get_summary_status(record,cfg)[0],'PASS')
        record['reconciliation_failures']=1
        self.assertEqual(tool.get_summary_status(record,cfg)[0],'REVIEW')
        cfg['thresholds']['max_reconciliation_failures']=0
        self.assertEqual(tool.get_summary_status(record,cfg)[0],'FAIL')

    def test_early_stop_on_first_candidate_not_all_columns(self):
        class Expression:
            def __init__(self,name): self.name=name
            def alias(self,name): return self
        class FakeData:
            def __init__(self, counts): self.counts=counts; self.visited=[]
            def agg(self,exp):
                self.visited.append(exp.name)
                return types.SimpleNamespace(first=lambda: {'n':self.counts[exp.name]})
        first=FakeData({'BAD':9,'EARLY':10,'LATER':10})
        second=FakeData({'EARLY':10,'LATER':10})
        with patch.object(tool.F,'countDistinct',side_effect=lambda name:Expression(name),create=True),\
             patch.object(tool,'qcol',side_effect=lambda name:name):
            self.assertEqual(tool.first_unique_key(first,second,['BAD','EARLY','LATER'],10,10),(['EARLY'],'EARLY'))
        self.assertEqual(first.visited,['BAD','EARLY'])
        self.assertEqual(second.visited,['EARLY'])


class DashboardTests(unittest.TestCase):
    @staticmethod
    def summary():
        return {'status':'REVIEW','status_reasons':['Unapproved differences'],
                'version':'4.0', 'run_dir':'file:///tmp/runs/R001',
                'file1_rows':100,'file2_rows':100,'matched_keys':99,
                'mismatched_records':2,'mismatch_pct_of_matched':2.0202,
                'missing_rows':2,'missing_from_file1':1,'missing_from_file2':1,
                'reconciliation_failures':0,'reconciliation_groups':1,
                'invalid_conversions':0,'schema_difference_count':1,
                'schema_differences':[{'Column':'NEW','Issue':'MISSING_IN_FILE1','File1_type':None,'File2_type':'string'}],
                'columns_compared':['AMOUNT','STATUS'], 'numeric_columns':['AMOUNT'],
                'excluded_columns':[], 'primary_key':'ACCOUNT_ID', 'pk_strategy':'first unique; early exit',
                'column_mismatch_summary':[
                    {'Column_affected':'STATUS','Mismatch_rows':1,'Mismatch_pct_of_matched':1.01,'Null_mismatch_rows':0},
                    {'Column_affected':'AMOUNT','Mismatch_rows':2,'Mismatch_pct_of_matched':2.02,'Null_mismatch_rows':1,
                     'Max_absolute_difference':'50.00'}],
                'timings_seconds':{'row_counts_and_key_detection':0.7,'difference_reports_and_preview':3.1},
                'inputs':{'File1':'Bigplay','File2':'DaaS'},
                'dashboard_sample_limit':20}

    def test_all_resources_embedded_no_network(self):
        page=render_dashboard(self.summary(),[{'PK':'001','Column_affected':'AMOUNT','File1':'120','File2':'150',
                                               'Difference_type':'NUMERIC_DIFFERENCE','Numeric_Difference':'30'}])
        self.assertIn('<!doctype html>',page)
        self.assertIn('<style>',page)
        self.assertIn('<script>',page)
        self.assertIn("connect-src 'none'",page)
        self.assertNotIn('https://cdn',page)
        self.assertIn('Amount'.upper(),page)
        self.assertIn('001',page)
        self.assertIn('data-column=',page)
        self.assertIn('row_counts_and_key_detection'.replace('_',' ').title(),page)

    def test_escapes_injected_html_and_raw_values(self):
        item={'PK':'<img src=x onerror=alert(1)>','Column_affected':'AMOUNT',
              'File1':'</script><script>alert(1)</script>','File2':'ok'}
        page=render_dashboard(self.summary(),[item])
        self.assertNotIn('<img src=x',page)
        self.assertNotIn('</script><script>alert(1)</script>',page)
        self.assertIn('&lt;img',page)
        self.assertIn('&lt;/script&gt;',page)

    def test_empty_dashboard_and_column_cap(self):
        data=self.summary(); data['column_mismatch_summary']=[]
        self.assertIn('No mismatched columns',render_dashboard(data,[],max_columns=0))
        data=self.summary()
        self.assertIn('Only first 1 of 2 columns shown',render_dashboard(data,[],max_columns=1))

    def test_offline_generation_cli(self):
        from data_validation_dashboard import main
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'summary.json';out=Path(d)/'result.html'
            p.write_text(json.dumps(self.summary()))
            self.assertEqual(main(['--summary',str(p),'--output',str(out)]),0)
            self.assertIn('Comparison overview',out.read_text())


if __name__=='__main__':
    unittest.main()
