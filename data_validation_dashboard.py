#!/usr/bin/env python3
"""Offline, dependency-free HTML dashboard for Data Validation.

All CSS/JavaScript is embedded. Never loads external content or sends data over
network. Dashboard is intended for approved internal storage; sample values
may contain confidential source records.
"""
import argparse
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote


def h(value):
    return html.escape('' if value is None else str(value), quote=True)


def num(value, decimals=0):
    if value is None:
        return '—'
    try:
        return f'{float(value):,.{decimals}f}' if decimals else f'{int(value):,}'
    except (ValueError, TypeError, OverflowError):
        return h(value)


def pretty_seconds(value):
    try:
        return f'{float(value):,.3f}s'
    except (TypeError, ValueError):
        return '—'


def _render_simple_table(headers, rows, empty='None detected'):
    head = ''.join(f'<th scope="col">{h(x)}</th>' for x in headers)
    cells = ''.join('<tr>' + ''.join(f'<td>{h(c)}</td>' for c in row) + '</tr>' for row in rows)
    if not rows:
        cells = f'<tr><td colspan="{len(headers)}" class="muted">{h(empty)}</td></tr>'
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{cells}</tbody></table></div>'


def render_dashboard(summary, sample_rows=None, max_columns=500):
    """Return portable, self-contained HTML from bounded JSON-serializable values.

    No Spark dependency. This function intentionally does not embed every diff.
    Full output stays in Spark-written CSVs.
    """
    samples = list(sample_rows or [])
    cols_all = sorted(summary.get('column_mismatch_summary', []),
                      key=lambda row: (-int(row.get('Mismatch_rows', 0)),
                                       str(row.get('Column_affected', ''))))
    cols = cols_all[:max(0, int(max_columns))]
    largest = max((int(x.get('Mismatch_rows', 0)) for x in cols), default=1) or 1
    status = str(summary.get('status', 'UNKNOWN')).upper()
    cls = {'PASS': 'pass', 'FAIL': 'fail', 'REVIEW': 'review'}.get(status, 'review')
    key = summary.get('primary_key', '—')
    report_links = [
        ('Combined differences', 'DataValidation_differencesall_records.csv/'),
        ('Per-column differences', 'DataValidation_differences_by_column/'),
        ('Column summary', 'DataValidation_column_summary.csv/'),
        ('Missing records', 'DataValidation_differencesmissing_records.csv/'),
        ('Schema differences', 'DataValidation_schema_differences.csv/'),
        ('Invalid conversions', 'DataValidation_invalid_conversions.csv/'),
        ('Data quality', 'DataValidation_data_quality.csv/'),
        ('Aggregate reconciliation', 'DataValidation_aggregate_reconciliation.csv/'),
        ('JSON summary', 'DataValidation_summary.json'),
    ]
    link_html = ''.join(f'<li><a href="{quote(p, safe="/-_.")}">{h(label)}</a></li>' for label, p in report_links)

    def stat(title, value, hint=''):
        return (f'<div class="stat"><div class="stat-label">{h(title)}</div>'
                f'<div class="stat-value">{h(value)}</div><div class="stat-hint">{h(hint)}</div></div>')

    metrics = [
        ('File 1 rows', num(summary.get('file1_rows')), summary.get('inputs', {}).get('File1', 'File1')),
        ('File 2 rows', num(summary.get('file2_rows')), summary.get('inputs', {}).get('File2', 'File2')),
        ('Matched keys', num(summary.get('matched_keys')), 'Matched records before value checks'),
        ('Changed records', num(summary.get('mismatched_records')), f"{num(summary.get('mismatch_pct_of_matched'), 3)}% of matched"),
        ('Missing keys', num(summary.get('missing_rows')), f"From File1: {num(summary.get('missing_from_file1'))}  ·  File2: {num(summary.get('missing_from_file2'))}"),
        ('Columns affected', num(len(cols_all)), f"{num(len(summary.get('columns_compared', [])))} columns compared"),
        ('Reconciliation failures', num(summary.get('reconciliation_failures')), f"{num(summary.get('reconciliation_groups'))} groups checked"),
        ('Invalid conversions', num(summary.get('invalid_conversions')), f"{num(summary.get('schema_difference_count'))} schema issues"),
    ]
    stats_html = ''.join(stat(*x) for x in metrics)
    reason_html = ''.join('<li>' + h(r) + '</li>' for r in summary.get('status_reasons', []))
    if not reason_html:
        reason_html = '<li>No failing validation conditions recorded.</li>'

    col_rows = []
    for r in cols:
        count = int(r.get('Mismatch_rows', 0))
        name = str(r.get('Column_affected', ''))
        width = max(0, min(100, count / largest * 100))
        col_rows.append(f'<tr data-col="{h(name.casefold())}" data-count="{count}">'
                        f'<td><button type="button" class="columnpick" data-name="{h(name)}" title="Filter sample rows for this column">{h(name)}</button></td>'
                        f'<td class="num">{num(count)}</td>'
                        f'<td class="num">{num(r.get("Mismatch_pct_of_matched"), 3)}%</td>'
                        f'<td class="num">{num(r.get("Null_mismatch_rows"))}</td>'
                        f'<td class="num">{h(r.get("Max_absolute_difference") or "—")}</td>'
                        f'<td class="bar-cell"><div class="bar"><span style="width:{width:.2f}%"></span></div></td></tr>')
    if not col_rows:
        col_rows = ['<tr><td colspan="6" class="muted">No mismatched columns.</td></tr>']

    sample_html = []
    for item in samples:
        col = str(item.get('Column_affected', ''))
        pk = item.get('PK', '')
        sample_html.append(f'<tr data-column="{h(col.casefold())}" data-search="{h(" ".join(str(item.get(k) or "") for k in ("PK", "Column_affected", "File1", "File2", "Difference_type")).casefold())}">'
                           + ''.join(f'<td>{h(item.get(k))}</td>' for k in
                                     ('PK','Column_affected','File1','File2','Difference_type','Numeric_Difference','Percentage_Difference'))
                           + '</tr>')
    if not sample_html:
        sample_html = ['<tr><td colspan="7" class="muted">No sample rows available.</td></tr>']

    schema_rows = [(r.get('Column'), r.get('Issue'), r.get('File1_type'), r.get('File2_type'))
                   for r in summary.get('schema_differences', [])]
    timings = summary.get('timings_seconds', {})
    time_rows = [(label.replace('_', ' ').title(), pretty_seconds(seconds))
                 for label, seconds in timings.items()]
    runid = str(summary.get('run_dir', '')).rstrip('/').split('/')[-1]
    version = summary.get('version', '4.0')
    sample_cap = summary.get('dashboard_sample_limit', 0)
    truncated = len(cols_all) > len(cols)
    truncated_note = f' Only first {len(cols)} of {len(cols_all)} columns shown.' if truncated else ''
    generated = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    css = '''
:root{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#17243a;background:#f5f7fa;line-height:1.5}
*{box-sizing:border-box}body{margin:0}main{max-width:1280px;margin:auto;padding:30px 20px 65px}h1{font-size:28px;margin:8px 0 4px}h2{font-size:19px;margin:0 0 15px}p{margin:7px 0 13px}.muted{color:#617186}.small{font-size:13px}.eyebrow{font-size:12px;letter-spacing:.11em;text-transform:uppercase;font-weight:800;color:#536b8f}
header{display:flex;gap:20px;align-items:flex-start;justify-content:space-between;margin-bottom:22px}.pill{display:inline-block;border-radius:100px;padding:9px 16px;font-size:13px;font-weight:800}.pill.pass{color:#075c41;background:#d6f6e7}.pill.fail{color:#842b29;background:#ffe5e2}.pill.review{color:#815900;background:#fff0c7}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:20px 0}.stat{background:white;border:1px solid #e0e6ee;padding:15px;border-radius:12px;min-width:0}.stat-label{font-size:12px;color:#617186;font-weight:700}.stat-value{font-size:27px;font-weight:800;white-space:nowrap;font-variant-numeric:tabular-nums}.stat-hint{font-size:11px;color:#617186;overflow-wrap:anywhere}
.card{background:white;border:1px solid #e0e6ee;border-radius:12px;padding:19px;margin:14px 0;box-shadow:0 2px 5px #1b2a3d06}.flex{display:flex;gap:14px;align-items:center;justify-content:space-between;flex-wrap:wrap}.cols{display:grid;grid-template-columns:1.2fr 1fr;gap:14px}.code{font-family:ui-monospace,monospace;font-size:12px;overflow-wrap:anywhere}table{border-collapse:collapse;width:100%;font-size:13px}th{background:#f0f4f9;text-align:left;color:#40546e;font-size:12px;font-weight:800;position:sticky;top:0;white-space:nowrap}th,td{border-bottom:1px solid #e8edf4;padding:10px 12px;vertical-align:top;overflow-wrap:anywhere}tr:last-child td{border-bottom:0}td.num{font-variant-numeric:tabular-nums;text-align:right}th.num{text-align:right}.scroll{max-height:420px;overflow:auto;border-radius:8px;border:1px solid #e8edf4}input{border:1px solid #cbd5e1;border-radius:8px;padding:9px 11px;font-size:13px;min-width:200px;background:white;color:#17243a}.columnpick{background:transparent;border:0;color:#1b5cad;cursor:pointer;font:inherit;padding:0;text-align:left;text-decoration:underline;text-underline-offset:3px}.bar{height:9px;background:#e8eef6;border-radius:9px;min-width:70px}.bar span{display:block;height:100%;border-radius:9px;background:#2f6fb0}a{color:#1c5fa6}li{margin:5px 0}.path-links{display:grid;grid-template-columns:repeat(3,1fr);gap:4px 15px;padding-left:20px}.note{background:#eef3f9;border-left:4px solid #5b80b0;padding:10px 14px;font-size:13px;border-radius:5px}button.reset{border:1px solid #ccd6e3;background:#f7faff;color:#264969;border-radius:8px;padding:9px 12px;cursor:pointer}.count{font-size:12px;color:#617186}#sample-note{padding:8px 0}.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.cols{grid-template-columns:1fr}.path-links{grid-template-columns:1fr 1fr}}@media(max-width:570px){header{flex-direction:column}.grid{grid-template-columns:1fr 1fr}.stat-value{font-size:22px}main{padding:16px 12px}.path-links{grid-template-columns:1fr}.scroll{max-height:350px}}
@media print{body{background:white}.card,.stat{box-shadow:none;break-inside:avoid}.scroll{overflow:visible;max-height:none}input,.reset,.columnpick{display:none}}
'''
    js = '''
(function(){"use strict";
 const colSearch=document.getElementById('column-search'),sampleSearch=document.getElementById('sample-search');
 const colRows=[...document.querySelectorAll('#columns tbody tr[data-col]')];
 const diffRows=[...document.querySelectorAll('#samples tbody tr[data-column]')];
 let selected='';
 function refresh(){const c=colSearch.value.trim().toLowerCase(),s=sampleSearch.value.trim().toLowerCase();let nc=0,nd=0;
 for(const r of colRows){const match=r.dataset.col.includes(c);r.hidden=!match;if(match)nc++}
 for(const r of diffRows){const match=(!selected||r.dataset.column===selected)&&r.dataset.search.includes(s);r.hidden=!match;if(match)nd++}
 document.getElementById('column-count').textContent=nc+' displayed';
 document.getElementById('sample-count').textContent=nd+' displayed';
 document.getElementById('sample-note').textContent=selected?'Sample filtered to '+selected+'. These are sampled rows only; full details are in per-column CSV files.':'Bounded preview only; it is not a complete set of discrepancy records.';
 }
 document.querySelectorAll('.columnpick').forEach(b=>b.addEventListener('click',()=>{selected=b.dataset.name.toLowerCase();document.getElementById('samples').scrollIntoView({behavior:'smooth',block:'start'});refresh()}));
 document.getElementById('reset').addEventListener('click',()=>{selected='';sampleSearch.value='';refresh()});
 colSearch.addEventListener('input',refresh);sampleSearch.addEventListener('input',refresh);refresh();
})();
'''
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src 'none'; connect-src 'none'">
<title>Data Validation V{h(version)} — Run {h(runid)}</title><style>{css}</style></head><body><main>
<header><div><div class="eyebrow">Data Validations · regression report</div><h1>Comparison overview</h1>
<p class="muted small">Run <span class="code">{h(runid)}</span> · Generated {h(generated)} · Offline, self-contained report</p></div>
<span class="pill {cls}">{h(status)}</span></header>
<section class="grid" aria-label="Key validation metrics">{stats_html}</section>
<div class="cols"><section class="card"><h2>Validation outcome</h2><ul>{reason_html}</ul>
<p class="small muted"><strong>Comparison key:</strong> <span class="code">{h(key)}</span></p>
<p class="small muted">{h(summary.get('pk_strategy',''))}</p></section>
<section class="card"><h2>Data and settings</h2>
<p class="small"><strong>File1:</strong> <span class="code">{h(summary.get('inputs',{}).get('File1',''))}</span></p>
<p class="small"><strong>File2:</strong> <span class="code">{h(summary.get('inputs',{}).get('File2',''))}</span></p>
<p class="small"><strong>Numeric columns:</strong> {h(', '.join(summary.get('numeric_columns', [])) or 'None')}</p>
<p class="small"><strong>Excluded columns:</strong> {h(', '.join(summary.get('excluded_columns', [])) or 'None')}</p></section></div>
<section class="card"><div class="flex"><div><h2>Discrepancies by column</h2><p class="muted small">Ranked by mismatch count. Click a column to filter the sample below.{h(truncated_note)}</p></div>
<div><label class="sr-only" for="column-search">Find column</label><input id="column-search" type="search" placeholder="Search columns" aria-label="Search columns"><span id="column-count" class="count" aria-live="polite"></span></div></div>
<div class="scroll"><table id="columns"><thead><tr><th>Column</th><th class="num">Rows</th><th class="num">% of matched</th><th class="num">NULL mismatches</th><th class="num">Max absolute Δ</th><th>Relative frequency</th></tr></thead><tbody>{''.join(col_rows)}</tbody></table></div></section>
<section class="card" id="samples"><div class="flex"><div><h2>Value-level discrepancy sample</h2><p class="muted small">Up to {num(sample_cap)} rows across all affected columns. Filterable, not exhaustive.</p></div>
<div><label class="sr-only" for="sample-search">Search sample</label><input id="sample-search" type="search" placeholder="Search sample records" aria-label="Search sample records"> <button type="button" class="reset" id="reset">Clear column filter</button> <span id="sample-count" class="count" aria-live="polite"></span></div></div>
<p id="sample-note" class="muted small">Preview only.</p><div class="scroll"><table id="samples-table"><thead><tr><th>PK</th><th>Column affected</th><th>File1</th><th>File2</th><th>Type</th><th>File2 − File1</th><th>% difference</th></tr></thead><tbody>{''.join(sample_html)}</tbody></table></div></section>
<div class="cols"><section class="card"><h2>Schema differences</h2>
{_render_simple_table(('Column','Finding','File1 type','File2 type'),schema_rows)}</section>
<section class="card"><h2>Timing breakdown</h2>{_render_simple_table(('Stage','Wall time'),time_rows,'No timings available')}<p class="small muted">Times are per-stage wall-clock measurements, not Spark executor CPU time.</p></section></div>
<section class="card"><h2>Output artifacts</h2><p class="small muted">Relative paths below point to CSV output folders (Spark writes part files). On HDFS or object storage, view them through an approved filesystem browser instead of local file links.</p>
<ul class="path-links">{link_html}</ul></section>
<section class="note"><strong>Confidentiality:</strong> This offline report embeds actual keys, input paths, and sampled source values. Store it in an approved location. No external fonts, scripts, analytics, or network requests are used. Full discrepancy files are authoritative.</section>
</main><script>{js}</script></body></html>'''


def main(argv=None):
    parser = argparse.ArgumentParser(description='Generate an offline HTML report from a DataValidation_summary.json file')
    parser.add_argument('--summary', required=True, help='Local JSON summary path')
    parser.add_argument('--output', required=True, help='HTML output path')
    parser.add_argument('--samples', help='Optional local JSON array of sampled discrepancies')
    parser.add_argument('--max-columns', type=int, default=500)
    args = parser.parse_args(argv)
    summary = json.loads(Path(args.summary).read_text(encoding='utf-8'))
    samples = json.loads(Path(args.samples).read_text(encoding='utf-8')) if args.samples else []
    html_doc = render_dashboard(summary, samples, args.max_columns)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(html_doc, encoding='utf-8')
    print(f'Created {args.output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
