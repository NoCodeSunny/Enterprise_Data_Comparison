# -*- coding: utf-8 -*-
"""DataComparePro Flask application — serves both legacy /api/* and new /api/v1/*."""
from __future__ import annotations
import io, json, os, queue, sqlite3, sys, threading, traceback, uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

_UI_ROOT = Path(__file__).parent.parent
_FW_ROOT = _UI_ROOT.parent / "edcp"
for _p in (str(_UI_ROOT), str(_FW_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

os.environ.setdefault("EDCP_REPORT_ROOT", str(_UI_ROOT / "reports"))

from flask import Flask, jsonify, request, send_file, Response, abort, send_from_directory
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024

# Register v1 blueprint
try:
    import importlib, sys as _sys
    _FW2 = str(Path(__file__).parent.parent.parent.parent / "edcp")
    _UI2 = str(Path(__file__).parent.parent)
    for _pp in (_FW2, _UI2):
        if _pp not in _sys.path: _sys.path.insert(0, _pp)
    _v1_mod = importlib.import_module('api.v1.batch_api')
    _v1_bp  = _v1_mod.v1
    _gbm    = _v1_mod.get_batch_manager
    app.register_blueprint(_v1_bp)
    _batch_mgr = _gbm()
    print("[app] /api/v1/ batch API registered")
except Exception as _e:
    _batch_mgr = None
    print(f"[app] WARNING: v1 batch API not loaded: {_e}")

@app.after_request
def _cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    r.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    return r

@app.route("/api/<path:p>", methods=["OPTIONS"])
def _pre(p): return "", 204

DB_PATH = Path(__file__).parent / "jobs.db"

def _db():
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def _init_db():
    with _db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS jobs(job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'QUEUED',created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,prod_path TEXT,dev_path TEXT,result_name TEXT,
            request_cfg TEXT,summary TEXT,report_xlsx TEXT,report_html TEXT,
            report_json TEXT,error_msg TEXT,debug_report TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS job_logs(id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,ts TEXT NOT NULL,level TEXT NOT NULL,message TEXT NOT NULL)""")
        c.commit()
_init_db()

_lqs: Dict[str, queue.Queue] = {}
_lk  = threading.Lock()

def _lq(jid):
    with _lk:
        if jid not in _lqs: _lqs[jid] = queue.Queue(maxsize=10000)
        return _lqs[jid]

def _log(jid, msg, lvl="INFO"):
    ts = datetime.utcnow().isoformat()
    try:
        with _db() as c:
            c.execute("INSERT INTO job_logs(job_id,ts,level,message) VALUES(?,?,?,?)",(jid,ts,lvl,msg))
            c.commit()
    except Exception: pass
    try: _lq(jid).put_nowait({"ts":ts,"level":lvl,"message":msg})
    except queue.Full: pass

def _upd(jid, **kw):
    if not kw: return
    kw["updated_at"] = datetime.utcnow().isoformat()
    with _db() as c:
        c.execute(f"UPDATE jobs SET {','.join(f'{k}=?' for k in kw)} WHERE job_id=?",
                  list(kw.values())+[jid]); c.commit()

def _run_job(jid, cfg):
    _log(jid,"Job started"); _upd(jid,status="RUNNING")
    try:
        from edcp.jobs.comparison_job import ComparisonJob
        from edcp.loaders.file_loader import load_any_to_csv
        pp=cfg["prod_path"]; dp=cfg["dev_path"]
        rn=cfg.get("result_name",f"Compare_{jid[:8]}")
        rr=Path(cfg.get("report_root",str(_UI_ROOT/"reports"/jid)))
        rr.mkdir(parents=True,exist_ok=True)
        tol={}; pn=Path(pp).name; dn=Path(dp).name
        for r in cfg.get("tolerance_rules",[]):
            f,d=r.get("field"),r.get("decimals")
            if f and d is not None: tol[(pn,dn,f)]=int(d)
        caps={**{"parquet":False,"comparison":True,"tolerance":True,"duplicate":True,
                 "schema":True,"data_quality":True,"audit":True,"alerts":False,"plugins":False},
              **{k:bool(v) for k,v in cfg.get("capabilities",{}).items()}}
        conv=rr/"converted"; conv.mkdir(exist_ok=True)
        pc=load_any_to_csv(Path(pp),conv); dc=load_any_to_csv(Path(dp),conv)
        job=ComparisonJob(prod_path=pc,dev_path=dc,prod_name=pn,dev_name=dn,
            result_name=rn,report_root=rr,keys=cfg.get("keys",[]),
            ignore_fields=cfg.get("ignore_fields",[]),tol_map=tol,
            capabilities_cfg=caps,alert_rules=cfg.get("alert_rules",[]),max_retries=0,
            config={"use_spark":cfg.get("use_spark",False)})
        res=job.run()
        if res.succeeded:
            _log(jid,f"✅ passed={res.summary.get('MatchedPassed',0)} failed={res.summary.get('MatchedFailed',0)}")
            from edcp.reporting.html_report import write_final_html
            from edcp.reporting.json_audit import write_json_audit
            write_final_html([res.to_summary_dict()],rr,res.elapsed_s)
            write_json_audit([res.to_summary_dict()],rr,res.elapsed_s)
            _upd(jid,status="SUCCESS",summary=json.dumps(res.to_summary_dict(),default=str),
                 report_xlsx=str(res.report_path or ""),
                 report_html=str(rr/"Final_Comparison_Summary.html"),
                 report_json=str(rr/"run_audit.json"))
        else:
            dbg=""
            if res.debug_report: dbg=json.dumps(res.debug_report.to_dict(),default=str)
            _log(jid,f"❌ {res.error}","ERROR"); _upd(jid,status="FAILED",error_msg=str(res.error),debug_report=dbg)
    except Exception as e:
        _log(jid,f"❌ {e}","ERROR"); _log(jid,traceback.format_exc(),"ERROR")
        dbg=""
        try:
            from edcp.debugger import Debugger
            dbg=json.dumps(Debugger().diagnose(e,context_hint=jid).to_dict(),default=str)
        except Exception: pass
        _upd(jid,status="FAILED",error_msg=str(e),debug_report=dbg)
    finally:
        _log(jid,"DONE")
        try: _lq(jid).put_nowait(None)
        except queue.Full: pass

# Legacy API
@app.route("/api/health")
def health():
    return jsonify({"status":"ok","version":"2.0","platform":"DataComparePro",
                    "timestamp":datetime.utcnow().isoformat()})

@app.route("/api/detect-columns",methods=["POST"])
def detect_cols():
    d=request.get_json(force=True,silent=True) or {}
    fp=d.get("path","").strip()
    if not fp: return jsonify({"error":"path required"}),400
    p=Path(fp)
    if not p.exists(): return jsonify({"error":f"File not found: {fp}","columns":[]}),404
    try:
        import pandas as pd
        from edcp.loaders.encoding import read_csv_robust,detect_delimiter
        ext=p.suffix.lower()
        if ext in(".xlsx",".xls"): df=pd.read_excel(str(p),nrows=0,engine="openpyxl")
        elif ext in(".parquet",".parq"):
            try:
                import pyarrow.parquet as pq
                return jsonify({"columns":list(pq.read_schema(str(p)).names),"file_size":p.stat().st_size})
            except Exception: df=pd.read_parquet(str(p)).head(0)
        else:
            delim=detect_delimiter(p); df=read_csv_robust(p,dtype=str,nrows=0,delimiter=delim)
        return jsonify({"columns":[str(c).strip() for c in df.columns],"file_size":p.stat().st_size})
    except Exception as e: return jsonify({"error":str(e),"columns":[]}),500

@app.route("/api/run",methods=["POST"])
def submit_run():
    d=request.get_json(force=True,silent=True) or {}
    if not d.get("prod_path") or not d.get("dev_path"):
        return jsonify({"error":"prod_path and dev_path required"}),400
    jid=uuid.uuid4().hex[:12]; now=datetime.utcnow().isoformat()
    rn=d.get("result_name") or f"Compare_{jid[:8]}"
    with _db() as c:
        c.execute("""INSERT INTO jobs(job_id,status,created_at,updated_at,prod_path,dev_path,
            result_name,request_cfg) VALUES(?,?,?,?,?,?,?,?)""",
            (jid,"QUEUED",now,now,d["prod_path"],d["dev_path"],rn,json.dumps(d))); c.commit()
    threading.Thread(target=_run_job,args=(jid,d),daemon=True).start()
    return jsonify({"job_id":jid,"status":"QUEUED","result_name":rn}),202

@app.route("/api/job/<jid>")
def get_job(jid):
    with _db() as c: row=c.execute("SELECT * FROM jobs WHERE job_id=?",(jid,)).fetchone()
    if not row: return jsonify({"error":"Not found"}),404
    r=dict(row)
    for f in("summary","request_cfg","debug_report"):
        if r.get(f):
            try: r[f]=json.loads(r[f])
            except Exception: pass
    if r.get("debug_report") and isinstance(r["debug_report"],dict):
        er=r["debug_report"].get("error_record",{})
        r["debug_summary"]={"error_code":er.get("error_code",""),"category":er.get("category",""),
            "human_message":er.get("human_message",""),"recommended_fix":er.get("recommended_fix","")}
    return jsonify(r)

@app.route("/api/job/<jid>/logs")
def get_logs(jid):
    with _db() as c:
        rows=c.execute("SELECT ts,level,message FROM job_logs WHERE job_id=? ORDER BY id",(jid,)).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/job/<jid>/stream")
def stream_logs(jid):
    def gen():
        with _db() as c:
            rows=c.execute("SELECT ts,level,message FROM job_logs WHERE job_id=? ORDER BY id",(jid,)).fetchall()
        for row in rows: yield f"data: {json.dumps(dict(row))}\n\n"
        q=_lq(jid)
        while True:
            try:
                msg=q.get(timeout=30)
                if msg is None: yield 'data: {"__done__":true}\n\n'; break
                yield f"data: {json.dumps(msg)}\n\n"
            except queue.Empty: yield ": keepalive\n\n"
    return Response(gen(),mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.route("/api/history")
def history():
    lim=min(int(request.args.get("limit",50)),200); off=int(request.args.get("offset",0))
    st=request.args.get("status",""); w,p=[],[]
    if st: w.append("status=?"); p.append(st.upper())
    wc=("WHERE "+" AND ".join(w)) if w else ""
    with _db() as c:
        tot=c.execute(f"SELECT COUNT(*) FROM jobs {wc}",p).fetchone()[0]
        rows=c.execute(f"SELECT job_id,status,created_at,updated_at,prod_path,dev_path,"
            f"result_name,report_xlsx,report_html,report_json,error_msg FROM jobs {wc} "
            f"ORDER BY created_at DESC LIMIT ? OFFSET ?",p+[lim,off]).fetchall()
    jobs=[]
    for row in rows:
        d=dict(row)
        with _db() as c:
            sr=c.execute("SELECT summary FROM jobs WHERE job_id=?",(d["job_id"],)).fetchone()
        if sr and sr["summary"]:
            try:
                sm=json.loads(sr["summary"])
                d["matched_passed"]=sm.get("MatchedPassed",0); d["matched_failed"]=sm.get("MatchedFailed",0)
            except Exception: pass
        jobs.append(d)
    return jsonify({"total":tot,"jobs":jobs,"limit":lim,"offset":off})

@app.route("/api/download/<jid>/<rtype>")
def dl_report(jid,rtype):
    with _db() as c:
        row=c.execute("SELECT report_xlsx,report_html,report_json,result_name FROM jobs WHERE job_id=?",(jid,)).fetchone()
    if not row: return jsonify({"error":"Not found"}),404
    pm={"excel":row["report_xlsx"],"html":row["report_html"],"json":row["report_json"]}
    fp=pm.get(rtype.lower())
    if not fp or not Path(fp).exists(): return jsonify({"error":f"{rtype} not available"}),404
    nm={"excel":f"{row['result_name']}_Report.xlsx","html":f"{row['result_name']}_Summary.html",
        "json":f"{row['result_name']}_Audit.json"}
    return send_file(fp,as_attachment=True,download_name=nm.get(rtype.lower(),Path(fp).name))

@app.route("/api/job/<jid>",methods=["DELETE"])
def del_job(jid):
    with _db() as c:
        c.execute("DELETE FROM jobs WHERE job_id=?",(jid,))
        c.execute("DELETE FROM job_logs WHERE job_id=?",(jid,)); c.commit()
    return jsonify({"deleted":jid})

@app.route("/api/capabilities")
def list_caps():
    try:
        from edcp.registry.capability_registry import CapabilityRegistry
        caps=CapabilityRegistry().list_capabilities(); don={"comparison","tolerance","duplicate","schema","data_quality","audit"}
        return jsonify([{"name":c,"default":c in don,"label":c.replace("_"," ").title()} for c in caps])
    except Exception as e: return jsonify({"error":str(e)}),500

UI_STATIC = Path(__file__).parent.parent/"static"

@app.route("/")
@app.route("/<path:path>")
def serve_ui(path="index.html"):
    if path.startswith("api/"): abort(404)
    t=UI_STATIC/path
    if t.is_file(): return send_from_directory(str(UI_STATIC),path)
    return send_from_directory(str(UI_STATIC),"index.html")

if __name__=="__main__":
    import argparse; p=argparse.ArgumentParser()
    p.add_argument("--port",type=int,default=5000); p.add_argument("--debug",action="store_true")
    a=p.parse_args(); print(f"DataComparePro running on http://localhost:{a.port}")
    app.run(host="0.0.0.0",port=a.port,debug=a.debug,threaded=True)
