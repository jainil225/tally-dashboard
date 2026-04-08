"""
Tally Dashboard Server v2
Run: python server.py  →  http://localhost:8080
"""
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import os, json

# psycopg2cffi is a CFFI-based psycopg2 that works on Python 3.14+
# Falls back to standard psycopg2 if available (for local dev)
try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass
import psycopg2, psycopg2.extras
from datetime import datetime, date
import urllib.request, urllib.error

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.getenv("DATABASE_URL",
    "postgresql://neondb_owner:npg_4vZrz9mCTfgO"
    "@ep-silent-bonus-a8buxxhi-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
)
S = "tally_sync_v2"
GROQ_KEY = os.getenv("GROQ_API_KEY","")

def get_pg():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def fmt(n):
    n=float(n or 0); a=abs(n); s="-" if n<0 else ""
    if a>=1e7: return f"{s}₹{a/1e7:.2f} Cr"
    if a>=1e5: return f"{s}₹{a/1e5:.2f} L"
    if a==0:   return "₹0"
    return f"{s}₹{a:,.2f}"

def get_data_fy(company, cur):
    try:
        cur.execute(f"SELECT MIN(voucher_date) AS mn FROM {S}.voucher_entries WHERE company_name=%s",[company])
        row=cur.fetchone()
        if row and row["mn"]:
            d=row["mn"]; y,m=d.year,d.month; fs=y if m>=4 else y-1; fe=fs+1
            return f"{fs}-04-01",f"{fe}-04-01",f"1-Apr-{str(fs)[2:]} to 31-Mar-{str(fe)[2:]}"
    except: pass
    now=datetime.now(); y,m=now.year,now.month; fs=y if m>=4 else y-1; fe=fs+1
    return f"{fs}-04-01",f"{fe}-04-01",f"1-Apr-{str(fs)[2:]} to 31-Mar-{str(fe)[2:]}"

def pct(c,p):
    try: return 0 if not p else round(((float(c)-float(p))/abs(float(p)))*100)
    except: return 0

@app.route("/api/companies")
def companies():
    try:
        conn=get_pg(); cur=conn.cursor()
        cur.execute(f"SELECT id,name,last_sync_at,voucher_count,entry_count FROM {S}.companies ORDER BY name")
        rows=[dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"companies":rows})
    except Exception as e:
        return jsonify({"error":str(e),"companies":[]}),500

@app.route("/api/dashboard")
def dashboard():
    company=request.args.get("company","").strip()
    if not company:
        try:
            conn=get_pg(); cur=conn.cursor()
            cur.execute(f"SELECT name FROM {S}.companies ORDER BY id LIMIT 1")
            row=cur.fetchone(); cur.close(); conn.close()
            company=row["name"] if row else ""
        except: pass
    if not company: return jsonify({"error":"No company"}),404
    today=date.today().isoformat()
    try:
        conn=get_pg(); cur=conn.cursor()
        FY_START,FY_END,fy_label=get_data_fy(company,cur)

        cur.execute(f"SELECT COALESCE(SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END),0) AS v FROM {S}.voucher_entries WHERE company_name=%s AND LOWER(ledger_name) IN ('cash','petty cash','cash in hand')",[company])
        cash=float(cur.fetchone()["v"] or 0)

        cur.execute(f"SELECT COALESCE(SUM(ABS(closing_balance)),0) AS v FROM {S}.ledgers WHERE company_name=%s AND LOWER(parent) IN ('bank accounts','bank od a/c','bank od account','bank c.c.a/c.','bank term loan a/c.')",[company])
        bank=float(cur.fetchone()["v"] or 0)

        cur.execute(f"SELECT COALESCE(SUM(ABS(closing_balance)),0) AS v FROM {S}.ledgers WHERE company_name=%s AND LOWER(parent) IN ('sundry creditors','creditors','accounts payable')",[company])
        payables=float(cur.fetchone()["v"] or 0)

        cur.execute(f"""
            SELECT TO_CHAR(DATE_TRUNC('month',voucher_date),'Mon-YY') AS month,
                DATE_TRUNC('month',voucher_date) AS md,
                ROUND(COALESCE(SUM(CASE WHEN voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%' AND voucher_type NOT ILIKE '%%credit%%' AND entry_type='DR' THEN ABS(amount) END),0),2) AS sales,
                ROUND(COALESCE(SUM(CASE WHEN voucher_type ILIKE '%%receipt%%' AND entry_type='CR' THEN ABS(amount) END),0),2) AS receipt,
                ROUND(COALESCE(SUM(CASE WHEN voucher_type ILIKE '%%purchase%%' AND voucher_type NOT ILIKE '%%order%%' AND entry_type='CR' THEN ABS(amount) END),0),2) AS purchase,
                ROUND(COALESCE(SUM(CASE WHEN voucher_type ILIKE '%%payment%%' AND entry_type='DR' THEN ABS(amount) END),0),2) AS payment
            FROM {S}.voucher_entries WHERE company_name=%s AND voucher_date>=%s AND voucher_date<%s
            GROUP BY DATE_TRUNC('month',voucher_date) ORDER BY md ASC
        """,[company,FY_START,FY_END])
        trend=[{"month":r["month"],"sales":float(r["sales"] or 0),"receipt":float(r["receipt"] or 0),"purchase":float(r["purchase"] or 0),"payment":float(r["payment"] or 0)} for r in cur.fetchall()]

        ts=sum(t["sales"] for t in trend); tp=sum(t["purchase"] for t in trend)
        tr=sum(t["receipt"] for t in trend); tpy=sum(t["payment"] for t in trend)

        def q1m(typ,ilike,nilike,entry,interval=""):
            q=f"SELECT ROUND(COALESCE(SUM(ABS(amount)),0),2) AS v FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '{ilike}'"
            p=[company]
            for n in nilike: q+=f" AND voucher_type NOT ILIKE '{n}'"
            q+=f" AND entry_type='{entry}' AND DATE_TRUNC('month',voucher_date)=DATE_TRUNC('month',CURRENT_DATE{interval})"
            if typ=="sale": q+=" AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%'"
            cur.execute(q,p); return float(cur.fetchone()["v"] or 0)

        s_this=q1m("sale","%%sale%%",["%%order%%","%%credit%%"],"DR")
        s_last=q1m("sale","%%sale%%",["%%order%%","%%credit%%"],"DR","- INTERVAL '1 month'")
        p_this=q1m("pur","%%purchase%%",["%%order%%"],"CR")
        p_last=q1m("pur","%%purchase%%",["%%order%%"],"CR","- INTERVAL '1 month'")

        cur.execute(f"""
            WITH pb AS (
                SELECT ledger_name,
                    SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END) AS nb,
                    MAX(voucher_date::date) AS ltd
                FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%'
                  AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%'
                GROUP BY ledger_name HAVING SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END)>0
            )
            SELECT SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 0 AND 45 THEN nb ELSE 0 END) d0,
                   SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 46 AND 90 THEN nb ELSE 0 END) d1,
                   SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 91 AND 135 THEN nb ELSE 0 END) d2,
                   SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 136 AND 180 THEN nb ELSE 0 END) d3,
                   SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 181 AND 225 THEN nb ELSE 0 END) d4,
                   SUM(CASE WHEN CURRENT_DATE-ltd>225 THEN nb ELSE 0 END) d5,
                   SUM(nb) total,SUM(CASE WHEN CURRENT_DATE-ltd>45 THEN nb ELSE 0 END) overdue
            FROM pb
        """,[company])
        ag=dict(cur.fetchone() or {}); av={k:float(ag.get(k) or 0) for k in ag}

        cur.execute(f"""
            SELECT ledger_name AS name,ROUND(SUM(ABS(amount)),2) AS total,COUNT(DISTINCT voucher_number) AS bills
            FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%' AND voucher_type NOT ILIKE '%%credit%%' AND entry_type='DR'
              AND voucher_date>=%s AND voucher_date<%s AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%' AND ledger_name NOT ILIKE '%%discount%%' AND ledger_name NOT ILIKE '%%round%%'
            GROUP BY ledger_name ORDER BY SUM(ABS(amount)) DESC LIMIT 10
        """,[company,FY_START,FY_END])
        top_cust=[{"name":r["name"],"amount":float(r["total"] or 0),"bills":r["bills"]} for r in cur.fetchall()]

        cur.execute(f"""
            SELECT ledger_name AS name,ROUND(SUM(ABS(amount)),2) AS total,COUNT(DISTINCT voucher_number) AS bills
            FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%purchase%%' AND voucher_type NOT ILIKE '%%order%%' AND entry_type='CR'
              AND voucher_date>=%s AND voucher_date<%s AND ledger_name NOT ILIKE '%%purchase%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%'
            GROUP BY ledger_name ORDER BY SUM(ABS(amount)) DESC LIMIT 10
        """,[company,FY_START,FY_END])
        top_supp=[{"name":r["name"],"amount":float(r["total"] or 0),"bills":r["bills"]} for r in cur.fetchall()]

        cur.execute(f"""
            SELECT v.voucher_number,v.voucher_type AS type,v.narration AS particulars,
                ROUND(SUM(CASE WHEN e.entry_type='DR' THEN ABS(e.amount) ELSE 0 END),2) AS amount,v.voucher_date::TEXT AS date
            FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name
            WHERE v.company_name=%s AND v.voucher_date=%s
            GROUP BY v.voucher_number,v.voucher_type,v.narration,v.voucher_date ORDER BY v.voucher_number LIMIT 50
        """,[company,today])
        day_book=[{"voucher":r["voucher_number"],"type":r["type"],"particulars":r["particulars"] or "","amount":float(r["amount"] or 0),"date":r["date"]} for r in cur.fetchall()]

        cur.execute(f"SELECT name,COALESCE(closing_balance,0) AS balance FROM {S}.ledgers WHERE company_name=%s AND LOWER(parent) IN ('bank accounts','bank od a/c','bank od account') ORDER BY name",[company])
        bank_ledgers=[{"name":r["name"],"balance":float(r["balance"] or 0)} for r in cur.fetchall()]

        cur.execute(f"""
            WITH ls AS (SELECT ledger_name,MAX(voucher_date::date) AS lsd FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%' AND entry_type='DR' AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' GROUP BY ledger_name)
            SELECT COUNT(*) AS cnt FROM {S}.ledgers l LEFT JOIN ls ON ls.ledger_name=l.name WHERE l.company_name=%s AND LOWER(l.parent) ILIKE '%%debtor%%' AND (ls.lsd IS NULL OR ls.lsd<CURRENT_DATE-90)
        """,[company,company])
        inactive=int(cur.fetchone()["cnt"] or 0)

        cur.close(); conn.close()
        return jsonify({
            "company":company,"fy_label":fy_label,"fy_start":FY_START,"fy_end":FY_END,
            "summary":{"cash":{"amount":cash,"formatted":fmt(cash)},"bank":{"amount":bank,"formatted":fmt(bank)},"payables":{"amount":payables,"formatted":fmt(payables)},"net_assets":{"amount":cash+bank-payables,"formatted":fmt(cash+bank-payables)}},
            "sales":{"total":{"amount":ts,"formatted":fmt(ts)},"purchase_total":{"amount":tp,"formatted":fmt(tp)},"receipt_total":{"amount":tr,"formatted":fmt(tr)},"payment_total":{"amount":tpy,"formatted":fmt(tpy)},"this_month":{"amount":s_this,"formatted":fmt(s_this),"vs_last":pct(s_this,s_last)},"purchase_month":{"amount":p_this,"formatted":fmt(p_this),"vs_last":pct(p_this,p_last)},"trend":trend},
            "receivables":{"total":{"amount":av.get("total",0),"formatted":fmt(av.get("total",0))},"overdue":{"amount":av.get("overdue",0),"formatted":fmt(av.get("overdue",0))},"proj_15":{"amount":av.get("d1",0),"formatted":fmt(av.get("d1",0))},"proj_60":{"amount":av.get("d1",0)+av.get("d2",0),"formatted":fmt(av.get("d1",0)+av.get("d2",0))},"aging":[{"label":"0-45 Days","amount":av.get("d0",0),"formatted":fmt(av.get("d0",0))},{"label":"45-90 Days","amount":av.get("d1",0),"formatted":fmt(av.get("d1",0))},{"label":"90-135 Days","amount":av.get("d2",0),"formatted":fmt(av.get("d2",0))},{"label":"135-180 Days","amount":av.get("d3",0),"formatted":fmt(av.get("d3",0))},{"label":"180-225 Days","amount":av.get("d4",0),"formatted":fmt(av.get("d4",0))},{"label":">225 Days","amount":av.get("d5",0),"formatted":fmt(av.get("d5",0))}]},
            "top_customers":top_cust,"top_suppliers":top_supp,"day_book":day_book,"inactive_customers":inactive,"bank_ledgers":bank_ledgers,"today":today,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error":str(e)}),500

@app.route("/api/sales/detail")
def sales_detail():
    company=request.args.get("company",""); month=request.args.get("month","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",50)); offset=(page-1)*limit
    try:
        conn=get_pg(); cur=conn.cursor()
        FY_START,FY_END,_=get_data_fy(company,cur)
        if month:
            cur.execute(f"""
                SELECT v.voucher_number,v.voucher_date::TEXT AS date,v.narration,
                    STRING_AGG(DISTINCT CASE WHEN e.entry_type='DR' AND e.ledger_name NOT ILIKE '%%sale%%' AND e.ledger_name NOT ILIKE '%%gst%%' AND e.ledger_name NOT ILIKE '%%tax%%' THEN e.ledger_name END,', ') AS party,
                    ROUND(SUM(CASE WHEN e.entry_type='DR' THEN ABS(e.amount) ELSE 0 END),2) AS amount
                FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name
                WHERE v.company_name=%s AND v.voucher_type ILIKE '%%sale%%' AND v.voucher_type NOT ILIKE '%%order%%' AND v.voucher_type NOT ILIKE '%%credit%%'
                  AND TO_CHAR(v.voucher_date,'Mon-YY')=%s
                GROUP BY v.voucher_number,v.voucher_date,v.narration ORDER BY v.voucher_date DESC LIMIT %s OFFSET %s
            """,[company,month,limit,offset])
        else:
            cur.execute(f"""
                SELECT v.voucher_number,v.voucher_date::TEXT AS date,v.narration,
                    STRING_AGG(DISTINCT CASE WHEN e.entry_type='DR' AND e.ledger_name NOT ILIKE '%%sale%%' AND e.ledger_name NOT ILIKE '%%gst%%' AND e.ledger_name NOT ILIKE '%%tax%%' THEN e.ledger_name END,', ') AS party,
                    ROUND(SUM(CASE WHEN e.entry_type='DR' THEN ABS(e.amount) ELSE 0 END),2) AS amount
                FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name
                WHERE v.company_name=%s AND v.voucher_type ILIKE '%%sale%%' AND v.voucher_type NOT ILIKE '%%order%%' AND v.voucher_type NOT ILIKE '%%credit%%'
                  AND v.voucher_date>=%s AND v.voucher_date<%s
                GROUP BY v.voucher_number,v.voucher_date,v.narration ORDER BY v.voucher_date DESC LIMIT %s OFFSET %s
            """,[company,FY_START,FY_END,limit,offset])
        rows=[{"voucher":r["voucher_number"],"date":r["date"],"party":r["party"] or "","narration":r["narration"] or "","amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"vouchers":rows,"page":page,"month":month})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error":str(e)}),500

@app.route("/api/purchase/detail")
def purchase_detail():
    company=request.args.get("company",""); month=request.args.get("month","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",50)); offset=(page-1)*limit
    try:
        conn=get_pg(); cur=conn.cursor()
        FY_START,FY_END,_=get_data_fy(company,cur)
        if month:
            cur.execute(f"""
                SELECT v.voucher_number,v.voucher_date::TEXT AS date,v.narration,
                    STRING_AGG(DISTINCT CASE WHEN e.entry_type='CR' AND e.ledger_name NOT ILIKE '%%purchase%%' AND e.ledger_name NOT ILIKE '%%gst%%' AND e.ledger_name NOT ILIKE '%%tax%%' THEN e.ledger_name END,', ') AS party,
                    ROUND(SUM(CASE WHEN e.entry_type='CR' THEN ABS(e.amount) ELSE 0 END),2) AS amount
                FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name
                WHERE v.company_name=%s AND v.voucher_type ILIKE '%%purchase%%' AND v.voucher_type NOT ILIKE '%%order%%'
                  AND TO_CHAR(v.voucher_date,'Mon-YY')=%s
                GROUP BY v.voucher_number,v.voucher_date,v.narration ORDER BY v.voucher_date DESC LIMIT %s OFFSET %s
            """,[company,month,limit,offset])
        else:
            cur.execute(f"""
                SELECT v.voucher_number,v.voucher_date::TEXT AS date,v.narration,
                    STRING_AGG(DISTINCT CASE WHEN e.entry_type='CR' AND e.ledger_name NOT ILIKE '%%purchase%%' AND e.ledger_name NOT ILIKE '%%gst%%' AND e.ledger_name NOT ILIKE '%%tax%%' THEN e.ledger_name END,', ') AS party,
                    ROUND(SUM(CASE WHEN e.entry_type='CR' THEN ABS(e.amount) ELSE 0 END),2) AS amount
                FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name
                WHERE v.company_name=%s AND v.voucher_type ILIKE '%%purchase%%' AND v.voucher_type NOT ILIKE '%%order%%'
                  AND v.voucher_date>=%s AND v.voucher_date<%s
                GROUP BY v.voucher_number,v.voucher_date,v.narration ORDER BY v.voucher_date DESC LIMIT %s OFFSET %s
            """,[company,FY_START,FY_END,limit,offset])
        rows=[{"voucher":r["voucher_number"],"date":r["date"],"party":r["party"] or "","narration":r["narration"] or "","amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"vouchers":rows,"page":page,"month":month})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/receivables/detail")
def receivables_detail():
    company=request.args.get("company",""); bucket=request.args.get("bucket","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",100)); offset=(page-1)*limit
    try:
        conn=get_pg(); cur=conn.cursor()
        bmap={"0-45":"BETWEEN 0 AND 45","45-90":"BETWEEN 46 AND 90","90-135":"BETWEEN 91 AND 135","135-180":"BETWEEN 136 AND 180","180-225":"BETWEEN 181 AND 225","225+":"> 225"}
        bsql=f"AND CURRENT_DATE-ltd {bmap[bucket]}" if bucket in bmap else ""
        cur.execute(f"""
            WITH pb AS (
                SELECT ledger_name,
                    SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END) AS nb,
                    MAX(voucher_date::date) AS ltd,
                    COUNT(DISTINCT voucher_number) AS bills,
                    ROUND(CASE WHEN SUM(ABS(amount))=0 THEN 0 ELSE SUM(ABS(amount)*(CURRENT_DATE-voucher_date::date))/SUM(ABS(amount)) END,0) AS avg_days
                FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%'
                  AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%'
                GROUP BY ledger_name HAVING SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END)>0
            )
            SELECT ledger_name AS party,nb AS amount,ltd::TEXT AS last_date,bills,avg_days,(CURRENT_DATE-ltd) AS overdue_days
            FROM pb WHERE TRUE {bsql} ORDER BY nb DESC LIMIT %s OFFSET %s
        """,[company,limit,offset])
        rows=[{"party":r["party"],"amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0)),"last_date":r["last_date"],"bills":r["bills"],"avg_days":int(r["avg_days"] or 0),"overdue_days":int(r["overdue_days"] or 0)} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"receivables":rows,"bucket":bucket,"page":page})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/voucher/entries")
def voucher_entries():
    company=request.args.get("company",""); voucher=request.args.get("voucher","")
    try:
        conn=get_pg(); cur=conn.cursor()
        cur.execute(f"SELECT ledger_name,entry_type,amount,voucher_type,voucher_date::TEXT AS date FROM {S}.voucher_entries WHERE company_name=%s AND voucher_number=%s ORDER BY entry_type",[company,voucher])
        rows=[{"ledger":r["ledger_name"],"type":r["entry_type"],"amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"entries":rows,"voucher":voucher})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/ledger/detail")
def ledger_detail():
    company=request.args.get("company",""); ledger=request.args.get("ledger","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",50)); offset=(page-1)*limit
    try:
        conn=get_pg(); cur=conn.cursor()
        cur.execute(f"""SELECT e.voucher_number,e.voucher_date::TEXT AS date,e.voucher_type AS type,e.entry_type,ABS(e.amount) AS amount FROM {S}.voucher_entries e WHERE e.company_name=%s AND e.ledger_name=%s ORDER BY e.voucher_date DESC, e.voucher_number DESC LIMIT %s OFFSET %s""",[company,ledger,limit,offset])
        rows=[{"voucher":r["voucher_number"],"date":r["date"],"type":r["type"],"entry_type":r["entry_type"],"amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
        cur.execute(f"""SELECT SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END) AS bal FROM {S}.voucher_entries WHERE company_name=%s AND ledger_name=%s""",[company,ledger])
        bal=float(cur.fetchone()["bal"] or 0)
        cur.close(); conn.close()
        return jsonify({"entries":rows,"balance":bal,"formatted":fmt(bal),"ledger":ledger,"page":page})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/daybook")
def daybook():
    company=request.args.get("company",""); dt=request.args.get("date",date.today().isoformat())
    try:
        conn=get_pg(); cur=conn.cursor()
        cur.execute(f"""
            SELECT v.voucher_number,v.voucher_type AS type,v.narration,
                ROUND(SUM(CASE WHEN e.entry_type='DR' THEN ABS(e.amount) ELSE 0 END),2) AS amount,v.voucher_date::TEXT AS date
            FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name
            WHERE v.company_name=%s AND v.voucher_date=%s GROUP BY v.voucher_number,v.voucher_type,v.narration,v.voucher_date ORDER BY v.voucher_number
        """,[company,dt])
        rows=[{"voucher":r["voucher_number"],"type":r["type"],"particulars":r["narration"] or "","amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0)),"date":r["date"]} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"entries":rows,"date":dt,"count":len(rows)})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/ai/chat",methods=["POST"])
def ai_chat():
    # AI is now called directly from the browser JS to Groq (avoids server-side 403 blocks)
    return jsonify({"error":"Direct browser call only"}),501

@app.route("/")
def index(): return send_from_directory(".","index.html")

@app.route("/<path:path>")
def static_files(path): return send_from_directory(".",path)

if __name__=="__main__":
    port=int(os.getenv("PORT",8080))
    print(f"\n  Tally Dashboard v2 → http://localhost:{port}\n")
    app.run(host="0.0.0.0",port=port,debug=False)
