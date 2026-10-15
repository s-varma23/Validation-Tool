"""Run in a Spark/PySpark environment. All tests are skipped otherwise.

Suggested: PYTHONPATH=. python -m unittest discover -s tests -v
"""
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from argparse import Namespace
from decimal import Decimal

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
HAS_SPARK=importlib.util.find_spec('pyspark') is not None

# The non-Spark unit module will install lightweight import stubs when Spark
# is absent. Integration code is only executed when real PySpark is installed.
if HAS_SPARK:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    import data_Validation_comp_v4 as tool
    from benchmark_data_validation_v4 import make_frames


@unittest.skipUnless(HAS_SPARK, 'Requires real PySpark and a working Java/Spark runtime')
class SparkValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.master('local[2]')
                   .appName('DataValidationV4_EndToEndTests')
                   .config('spark.ui.enabled','false')
                   .config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.conf.set('spark.sql.ansi.enabled','false')
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows, names):
        return self.spark.createDataFrame(rows,','.join(f'{c} string' for c in names))

    def test_first_valid_pk_real_spark(self):
        a=self.frame([('x','001','m'),('x','002','n'),('x','003','p')],['NOT_UNIQUE','ID','LATER'])
        b=self.frame([('x','001','m'),('y','002','n'),('x','003','p')],['NOT_UNIQUE','ID','LATER'])
        self.assertEqual(tool.first_unique_key(a,b,['NOT_UNIQUE','ID','LATER'],3,3),(['ID'],'ID'))

    def test_composite_join_prevents_delimiter_collisions(self):
        a=self.frame([('A+B','C','10'),('A','B+C','20')],['K1','K2','VALUE'])
        b=self.frame([('A','B+C','25'),('A+B','C','10')],['K1','K2','VALUE'])
        tool.validate_explicit_keys(a,['K1','K2'],'File1')
        rules={'VALUE':{'numeric':True,'tolerance':tool.dec('0'), 'scale':2,
                        'date_format':None,'case_sensitive':True,'trim_whitespace':False}}
        base,wide,long=tool.make_difference_frames(a,b,['K1','K2'],['VALUE'],rules)
        try:
            rows=long.collect()
            self.assertEqual(len(rows),1)
            self.assertIn('B+C',rows[0]['PK'])
            self.assertEqual(Decimal(str(rows[0]['Numeric_Difference'])), Decimal('5'))
        finally:
            base.unpersist()

    def test_missing_in_both_directions(self):
        a=self.frame([('001','A'),('002','B')],['ID','FIELD'])
        b=self.frame([('001','A'),('003','C')],['ID','FIELD'])
        rows=tool.missing_frame(a,b,['ID'],['ID','FIELD']).collect()
        self.assertEqual({(r['Missing_from'],r['PK']) for r in rows},
                         {('File2','002'),('File1','003')})

    def test_numeric_tolerance_text_and_nulls(self):
        a=self.frame([('1','100.00',' Active '),('2','9.00',None),('3','0','OPEN')],['ID','AMOUNT','STATUS'])
        b=self.frame([('1','100.01','active'),('2','8.90','Active'),('3','0','open')],['ID','AMOUNT','STATUS'])
        r={'numeric':True,'tolerance':tool.dec('0.02'),'scale':2,'date_format':None,'case_sensitive':True,'trim_whitespace':False}
        t={'numeric':False,'tolerance':tool.dec('0'),'scale':4,'date_format':None,'case_sensitive':False,'trim_whitespace':True}
        base,wide,long=tool.make_difference_frames(a,b,['ID'],['AMOUNT','STATUS'],{'AMOUNT':r,'STATUS':t})
        try:
            rows={(x['PK'],x['Column_affected']):x for x in long.collect()}
            self.assertNotIn(('1','AMOUNT'),rows)  # within tolerance
            self.assertNotIn(('1','STATUS'),rows)  # normalized case/trim
            self.assertNotIn(('3','STATUS'),rows)
            self.assertIn(('2','AMOUNT'),rows)
            self.assertEqual(rows[('2','STATUS')]['Difference_type'],'NULL_MISMATCH')
        finally: base.unpersist()

    def test_explicit_duplicate_or_null_composite_keys_rejected(self):
        dup=self.frame([('x','a','2'),('x','a','3')],['K1','K2','V'])
        null=self.frame([('x',None,'2')],['K1','K2','V'])
        with self.assertRaisesRegex(ValueError,'not unique'):
            tool.validate_explicit_keys(dup,['K1','K2'],'File1')
        with self.assertRaisesRegex(ValueError,'NULL'):
            tool.validate_explicit_keys(null,['K1','K2'],'File2')

    def test_aggregate_reconciliation_missing_group_and_null_group(self):
        a=self.frame([('AA','100.00'),(None,'50')],['GROUP','AMOUNT'])
        b=self.frame([('AA','110.00'),('XX','20'),(None,'50')],['GROUP','AMOUNT'])
        rows=tool.aggregate_reconciliation(a,b,[{'name':'group_total','group_by':['GROUP'],
                                                 'sum_columns':['AMOUNT'],'tolerance':'0.00'}]).collect()
        self.assertEqual(len(rows),3)
        self.assertEqual(sorted([x['Status'] for x in rows]),['FAIL','FAIL','PASS'])

    def test_input_csv_header_schema_and_invalid_type(self):
        with tempfile.TemporaryDirectory() as d:
            f=Path(d)/'input.csv'
            f.write_text('Account ID,Amount\n001,10.02\n002,NOT_NUMBER\n',encoding='utf-8')
            cfg={**tool.DEFAULTS,'infer_schema':False,'column_types':{'Amount':'decimal(12,2)'}}
            df,errors=tool.load_data(self.spark,str(f),cfg)
            self.assertEqual(df.columns,['Account_ID','Amount'])
            self.assertEqual(errors,[('Amount',1,'INVALID_EXPLICIT_TYPE')])
            self.assertEqual(df.where(F.col('Account_ID')=='001').first()['Account_ID'],'001')

    def test_full_pipeline_reports_and_dashboard(self):
        with tempfile.TemporaryDirectory() as d:
            f1=Path(d)/'bigplay.csv';f2=Path(d)/'daas.csv'
            f1.write_text('ID,AMOUNT,STATUS\n001,100.00,Active\n002,200.00,Active\n003,300.00,Pending\n',encoding='utf-8')
            f2.write_text('ID,AMOUNT,STATUS\n001,100.01,active\n002,220.00,Closed\n004,400.00,Active\n',encoding='utf-8')
            cfg=tool.configuration(Namespace(config=None,bigplay_file=str(f1),daas_file=str(f2),
                                             username=None,output_dir=d,composite=None,
                                             run_id='e2e',numeric_tolerance=None))
            cfg.update({'infer_schema':False,'numeric_columns':['AMOUNT'],
                        'rules':{'AMOUNT':{'numeric':True,'tolerance':'0.05'},
                                 'STATUS':{'case_sensitive':False}},
                        'reconciliations':[{'name':'total','sum_columns':['AMOUNT']}],
                        'spark':{**cfg['spark'],'shuffle_partitions':2,'output_partitions':2}})
            result=tool.run(self.spark,cfg)
            self.assertEqual(result['primary_key'],'ID')
            self.assertEqual(result['matched_keys'],2)
            self.assertEqual(result['mismatched_records'],1)
            self.assertEqual(result['missing_rows'],2)
            self.assertEqual(result['reconciliation_failures'],1)
            self.assertEqual(result['status'],'REVIEW')
            self.assertTrue(result['dashboard_generated'])
            path=Path(d)/'DataValidation_v4'/'e2e'
            page=(path/tool.DASHBOARD_FILE).read_text(encoding='utf-8')
            self.assertIn('Comparison overview',page)
            self.assertIn('AMOUNT',page)
            self.assertIn('220.00',page)
            self.assertIn('script-src',page)
            self.assertEqual(json.loads((path/tool.SUMMARY_FILE).read_text())['version'],'4.0')
            details=self.spark.read.option('header','true').csv(str(path/tool.COLUMN_OUTPUT))
            self.assertIn('PK',details.columns)
            self.assertIn('File1',details.columns)
            self.assertEqual(details.count(),2)  # 002 differs in amount and status
            missing=self.spark.read.option('header','true').csv(str(path/tool.MISSING_OUTPUT))
            self.assertEqual(missing.count(),2)

    def test_no_difference_dashboard_and_summary(self):
        with tempfile.TemporaryDirectory() as d:
            csv='ID,AMOUNT\n001,10\n002,20\n'
            f1=Path(d)/'a.csv';f2=Path(d)/'b.csv'
            f1.write_text(csv);f2.write_text(csv)
            cfg=tool.configuration(Namespace(config=None,bigplay_file=str(f1),daas_file=str(f2),
                                             username=None,output_dir=d,composite=None,
                                             run_id='equal',numeric_tolerance=None))
            cfg['infer_schema']=False
            result=tool.run(self.spark,cfg)
            self.assertEqual(result['status'],'PASS')
            self.assertEqual(result['mismatched_records'],0)
            self.assertTrue((Path(d)/'DataValidation_v4'/'equal'/tool.DASHBOARD_FILE).is_file())

    def test_explicit_key_missing_field_rejected(self):
        data=self.frame([('001','v')],['ID','VALUE'])
        with self.assertRaisesRegex(ValueError,'does not contain'):
            tool.validate_explicit_keys(data,['MISSING'],'File1')

    def test_automatic_pk_with_all_duplicates_rejected(self):
        a=self.frame([('x','a'),('x','a')],['A','B'])
        b=self.frame([('y','a'),('y','a')],['A','B'])
        with self.assertRaisesRegex(ValueError,'No unique'):
            tool.first_unique_key(a,b,['A','B'],2,2)

    def test_date_formats_and_invalid_conversions(self):
        a=self.frame([('1','2026/10/14','10'),('2','not-a-date','BAD')],['ID','DAY','AMOUNT'])
        b=self.frame([('1','2026-10-14','10'),('2','2026-10-15','BAD')],['ID','DAY','AMOUNT'])
        rules={'DAY':{'numeric':False,'date_format':'yyyy/MM/dd','scale':4,
                      'tolerance':tool.dec('0'), 'case_sensitive':True,'trim_whitespace':False},
               'AMOUNT':{'numeric':True,'date_format':None,'scale':2,
                         'tolerance':tool.dec('0'),'case_sensitive':True,'trim_whitespace':False}}
        conversion=tool.conversion_counts(a,b,['DAY','AMOUNT'],rules)
        self.assertIn(('File1','DAY',1,'INVALID_DATE'),conversion)
        self.assertIn(('File1','AMOUNT',1,'INVALID_NUMERIC'),conversion)
        self.assertIn(('File2','DAY',2,'INVALID_DATE'),conversion)

    def test_schema_difference_full_run_not_hidden(self):
        with tempfile.TemporaryDirectory() as d:
            left=Path(d)/'a.csv';right=Path(d)/'b.csv'
            left.write_text('ID,V\n01,100\n02,200\n')
            right.write_text('ID,V,NEW_COLUMN\n01,100,a\n02,200,b\n')
            cfg=tool.configuration(Namespace(config=None,bigplay_file=str(left),daas_file=str(right),
                                             username=None,output_dir=d,composite=None,
                                             run_id='schema',numeric_tolerance=None))
            cfg['infer_schema']=False
            result=tool.run(self.spark,cfg)
            self.assertEqual(result['schema_difference_count'],1)
            self.assertEqual(result['mismatched_records'],0)
            self.assertEqual(result['status'],'REVIEW')
            schema=Path(d)/'DataValidation_v4'/'schema'/tool.SCHEMA_OUTPUT
            self.assertEqual(self.spark.read.option('header','true').csv(str(schema)).count(),1)

    def test_threshold_status_makes_run_fail(self):
        with tempfile.TemporaryDirectory() as d:
            a=Path(d)/'a.csv';b=Path(d)/'b.csv'
            a.write_text('ID,V\n01,AA\n02,BB\n')
            b.write_text('ID,V\n01,XX\n02,BB\n')
            cfg=tool.configuration(Namespace(config=None,bigplay_file=str(a),daas_file=str(b),
                                             username=None,output_dir=d,composite=None,
                                             run_id='fail',numeric_tolerance=None))
            cfg.update(infer_schema=False,thresholds={'max_mismatch_records':0},
                       dashboard={'enabled':False,'sample_rows':0,'max_columns':100})
            result=tool.run(self.spark,cfg)
            self.assertEqual(result['status'],'FAIL')
            self.assertFalse(result['dashboard_generated'])
            self.assertFalse((Path(d)/'DataValidation_v4'/'fail'/tool.DASHBOARD_FILE).exists())

    def test_benchmark_synthetic_frames(self):
        a,b=make_frames(self.spark,100,3,2)
        self.assertEqual(a.columns,['ID','COL_000','COL_001','COL_002'])
        self.assertEqual(a.count(),100)
        self.assertEqual(b.count(),100)
        self.assertEqual(b.where(F.col('ID')=='101').count(),1)
        self.assertEqual(a.where(F.col('ID')=='1').count(),1)
        self.assertEqual(b.where(F.col('ID')=='1').count(),0)


if __name__=='__main__':
    unittest.main()
