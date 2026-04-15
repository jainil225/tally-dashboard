"""
BizSage Dashboard Server v3
Features: Auth, Sessions, Page Tracking, AI Chat Limit (2/day free users)
Run: python server.py  →  http://localhost:8080
"""
from flask import Flask, jsonify, request, send_from_directory, make_response
from flask_cors import CORS
import os, secrets

try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass
import psycopg2, psycopg2.extras
from datetime import datetime, date, timedelta

try:
    from passlib.hash import bcrypt as bcrypt_hash
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False
    import hashlib as _hl
    class bcrypt_hash:
        @staticmethod
        def hash(pw): return "sha256:" + _hl.sha256(pw.encode()).hexdigest()
        @staticmethod
        def verify(pw, h):
            if h.startswith("sha256:"):
                return h == "sha256:" + _hl.sha256(pw.encode()).hexdigest()
            return False

app = Flask(__name__)
CORS(app, supports_credentials=True)

# ══════════════════════════════════════════════════════════════════════════
#  DUAL DATABASE CONFIG
#  APP_DATABASE_URL  → app schema (auth, sessions, tracking)
#  TALLY_DATABASE_URL → tally_sync_v2 schema (vouchers, ledgers)
# ══════════════════════════════════════════════════════════════════════════

APP_DATABASE_URL = os.getenv("APP_DATABASE_URL",
    "postgresql://neondb_owner:npg_ovpfE3WcBid0"
    "@ep-proud-dew-anl6spm4-pooler.c-6.us-east-1.aws.neon.tech"
    "/neondb?sslmode=require"
)

TALLY_DATABASE_URL = os.getenv("TALLY_DATABASE_URL",
    "postgresql://neondb_owner:npg_ovpfE3WcBid0"
    "@ep-proud-dew-anl6spm4-pooler.c-6.us-east-1.aws.neon.tech"
    "/neondb?sslmode=require"
)

S = "tally_sync_v2"
SESSION_COOKIE = "td_session"
SESSION_DAYS   = 7

# ── CONNECTION POOLS (reuse connections instead of opening new one per request)
from psycopg2 import pool as pg_pool
import threading, time as _time

_app_pool   = None
_tally_pool = None
_pool_lock  = threading.Lock()

def _init_app_pool():
    global _app_pool
    if _app_pool is None:
        with _pool_lock:
            if _app_pool is None:
                try:
                    _app_pool = pg_pool.ThreadedConnectionPool(1, 5, APP_DATABASE_URL,
                        cursor_factory=psycopg2.extras.RealDictCursor)
                except Exception as e:
                    print("App pool init error:", e)
    return _app_pool

def _init_tally_pool():
    global _tally_pool
    if _tally_pool is None:
        with _pool_lock:
            if _tally_pool is None:
                try:
                    _tally_pool = pg_pool.ThreadedConnectionPool(1, 5, TALLY_DATABASE_URL,
                        cursor_factory=psycopg2.extras.RealDictCursor)
                except Exception as e:
                    print("Tally pool init error:", e)
    return _tally_pool

class _PoolConn:
    """Context manager: borrow from pool, return on exit."""
    def __init__(self, pool_fn, fallback_url):
        self._pool_fn = pool_fn
        self._fallback_url = fallback_url
        self.conn = None
        self._from_pool = False
    def __enter__(self):
        p = self._pool_fn()
        if p:
            try:
                self.conn = p.getconn()
                self._from_pool = True
                return self.conn
            except Exception:
                pass
        self.conn = psycopg2.connect(self._fallback_url,
            cursor_factory=psycopg2.extras.RealDictCursor)
        return self.conn
    def __exit__(self, exc_type, *_):
        if not self.conn: return
        try:
            if exc_type: self.conn.rollback()
            if self._from_pool:
                p = self._pool_fn()
                if p: p.putconn(self.conn); return
            self.conn.close()
        except Exception:
            try: self.conn.close()
            except: pass

def get_app_pg():
    return _PoolConn(_init_app_pool, APP_DATABASE_URL)

def get_tally_pg():
    return _PoolConn(_init_tally_pool, TALLY_DATABASE_URL)

# ── SESSION CACHE: avoid a DB hit on every single API request ──────────────
_session_cache = {}
_SESSION_TTL   = 30   # seconds

def _cache_get(token):
    e = _session_cache.get(token)
    if e and _time.monotonic() < e[1]: return e[0]
    _session_cache.pop(token, None); return None

def _cache_set(token, user):
    if len(_session_cache) > 500:
        now = _time.monotonic()
        for k in [k for k,v in list(_session_cache.items()) if now >= v[1]]:
            _session_cache.pop(k, None)
    _session_cache[token] = (user, _time.monotonic() + _SESSION_TTL)

def _cache_del(token):
    _session_cache.pop(token, None)

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

# ══════════════════════════════════════════════════════════════════════════
#  AUTH HELPERS  (all use get_app_pg)
# ══════════════════════════════════════════════════════════════════════════

def get_session_token():
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        auth = request.headers.get("Authorization","")
        if auth.startswith("Bearer "):
            token = auth[7:]
    return token

def get_current_user():
    token = get_session_token()
    if not token: return None
    # Fast path: return cached user (avoids DB hit on every request)
    cached = _cache_get(token)
    if cached: return cached
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT u.id, u.email, u.name, u.role, u.plan, u.ai_limit_day,
                       s.id AS session_id
                FROM app.sessions s
                JOIN app.users u ON u.id = s.user_id
                WHERE s.token = %s AND s.is_active = TRUE
                  AND s.expires_at > NOW() AND u.is_active = TRUE
            """, [token])
            user = cur.fetchone()
            if user:
                cur.execute("UPDATE app.sessions SET last_seen=NOW() WHERE token=%s", [token])
                conn.commit()
                u = dict(user)
                _cache_set(token, u)
                cur.close()
                return u
            cur.close()
            return None
    except Exception as e:
        print("Session check error:", e)
        return None

def require_auth(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized", "login_required": True}), 401
        request.current_user = user
        return f(*args, **kwargs)
    return decorated

def require_admin(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user or user["role"] != "admin":
            return jsonify({"error": "Forbidden"}), 403
        request.current_user = user
        return f(*args, **kwargs)
    return decorated

# ══════════════════════════════════════════════════════════════════════════
#  AUTH ROUTES  (all use get_app_pg)
# ══════════════════════════════════════════════════════════════════════════

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    email    = (data.get("email","")).strip().lower()
    password = data.get("password","")
    ip       = request.headers.get("X-Forwarded-For", request.remote_addr)
    ua       = request.headers.get("User-Agent","")

    if not email or not password:
        return jsonify({"error": "Email and password required"}), 400

    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM app.users WHERE email=%s AND is_active=TRUE", [email])
            user = cur.fetchone()
            success = bool(user and bcrypt_hash.verify(password, user["password_hash"]))
            cur.execute("""
                INSERT INTO app.login_events (email, user_id, success, ip_address, user_agent)
                VALUES (%s, %s, %s, %s, %s)
            """, [email, user["id"] if user else None, success, ip, ua[:500]])
            if not success:
                conn.commit(); cur.close()
                return jsonify({"error": "Invalid email or password"}), 401
            token = secrets.token_urlsafe(48)
            cur.execute("""
                INSERT INTO app.sessions (token, user_id, ip_address, user_agent, expires_at)
                VALUES (%s, %s, %s, %s, %s)
            """, [token, user["id"], ip, ua[:500], datetime.utcnow() + timedelta(days=SESSION_DAYS)])
            cur.execute("UPDATE app.users SET last_login_at=NOW() WHERE id=%s", [user["id"]])
            conn.commit(); cur.close()
        resp = make_response(jsonify({
            "ok": True, "token": token,
            "user": {"id": user["id"], "email": user["email"], "name": user["name"],
                     "role": user["role"], "plan": user["plan"], "ai_limit_day": user["ai_limit_day"]}
        }))
        resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_DAYS*86400,
                        httponly=True, samesite="Lax", secure=False)
        return resp
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/auth/logout", methods=["POST"])
def logout():
    token = get_session_token()
    if token:
        _cache_del(token)
        try:
            with get_app_pg() as conn:
                cur = conn.cursor()
                cur.execute("UPDATE app.sessions SET is_active=FALSE WHERE token=%s", [token])
                conn.commit(); cur.close()
        except: pass
    resp = make_response(jsonify({"ok": True}))
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.route("/api/auth/me")
def me():
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not logged in", "login_required": True}), 401
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS used FROM app.ai_chats WHERE user_id=%s AND chat_date=CURRENT_DATE", [user["id"]])
            used = int(cur.fetchone()["used"]); cur.close()
    except: used = 0
    return jsonify({"user": user, "ai_used_today": used, "ai_remaining": max(0, user["ai_limit_day"] - used)})


@app.route("/api/auth/register", methods=["POST"])
def register():
    if os.getenv("ALLOW_REGISTER","true").lower() == "false":
        return jsonify({"error": "Registration is disabled. Contact admin."}), 403
    data = request.get_json() or {}
    email = (data.get("email","")).strip().lower()
    name  = (data.get("name","")).strip()
    password = data.get("password","")
    if not email or not name or not password:
        return jsonify({"error": "All fields required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM app.users WHERE email=%s", [email])
            if cur.fetchone():
                cur.close()
                return jsonify({"error": "Email already registered"}), 409
            pw_hash = bcrypt_hash.hash(password)
            cur.execute("""INSERT INTO app.users (email, name, password_hash, role, plan, ai_limit_day)
                           VALUES (%s,%s,%s,'user','free',2) RETURNING id""", [email, name, pw_hash])
            conn.commit(); cur.close()
        return jsonify({"ok": True, "message": "Account created. Please log in."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════
#  TRACKING ROUTES  (all use get_app_pg)
# ══════════════════════════════════════════════════════════════════════════

@app.route("/api/track/page", methods=["POST"])
@require_auth
def track_page():
    data = request.get_json() or {}
    user = request.current_user
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("""INSERT INTO app.page_events (user_id, session_id, tab_name, company_name)
                           VALUES (%s,%s,%s,%s) RETURNING id""",
                        [user["id"], user["session_id"], data.get("tab",""), data.get("company","")])
            event_id = cur.fetchone()["id"]
            conn.commit(); cur.close()
        return jsonify({"ok": True, "event_id": event_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/track/page/exit", methods=["POST"])
@require_auth
def track_page_exit():
    data     = request.get_json() or {}
    event_id = data.get("event_id")
    duration = int(data.get("duration_sec", 0))
    if not event_id: return jsonify({"ok": True})
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("""UPDATE app.page_events SET exited_at=NOW(), duration_sec=%s
                           WHERE id=%s AND user_id=%s""",
                        [duration, event_id, request.current_user["id"]])
            conn.commit(); cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════
#  AI CHAT LIMIT  (uses get_app_pg)
# ══════════════════════════════════════════════════════════════════════════

@app.route("/api/ai/check")
@require_auth
def ai_check():
    user = request.current_user
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS used FROM app.ai_chats WHERE user_id=%s AND chat_date=CURRENT_DATE", [user["id"]])
            used = int(cur.fetchone()["used"]); cur.close()
        limit = user["ai_limit_day"]
        return jsonify({"used":used,"limit":limit,"remaining":max(0,limit-used),"allowed":used<limit,"plan":user["plan"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ai/log", methods=["POST"])
@require_auth
def ai_log():
    user = request.current_user
    data = request.get_json() or {}
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS used FROM app.ai_chats WHERE user_id=%s AND chat_date=CURRENT_DATE", [user["id"]])
            used = int(cur.fetchone()["used"])
            limit = user["ai_limit_day"]
            if used >= limit:
                cur.close()
                return jsonify({"allowed":False,"used":used,"limit":limit,"remaining":0,
                                "message":f"Daily limit of {limit} AI chats reached. Upgrade for unlimited access."}), 429
            cur.execute("""INSERT INTO app.ai_chats (user_id,session_id,mode,prompt,response_len,company_name)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        [user["id"], user["session_id"], data.get("mode","ca"),
                         data.get("prompt","")[:2000], int(data.get("response_len",0)), data.get("company","")])
            conn.commit(); cur.close()
        return jsonify({"allowed":True,"used":used+1,"limit":limit,"remaining":limit-used-1})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════
#  ADMIN ANALYTICS  (uses get_app_pg)
# ══════════════════════════════════════════════════════════════════════════

@app.route("/api/admin/users")
@require_admin
def admin_users():
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM app.v_user_summary ORDER BY total_sec DESC NULLS LAST")
            rows = [dict(r) for r in cur.fetchall()]; cur.close()
        return jsonify({"users": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/tab-time")
@require_admin
def admin_tab_time():
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM app.v_tab_time")
            rows = [dict(r) for r in cur.fetchall()]; cur.close()
        return jsonify({"tabs": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/ai-today")
@require_admin
def admin_ai_today():
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM app.v_ai_today ORDER BY chats_today DESC")
            rows = [dict(r) for r in cur.fetchall()]; cur.close()
        return jsonify({"ai_usage": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/daily-active")
@require_admin
def admin_daily():
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM app.v_daily_active LIMIT 30")
            rows = [dict(r) for r in cur.fetchall()]; cur.close()
        return jsonify({"daily": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/user/<int:uid>/limit", methods=["POST"])
@require_admin
def admin_set_limit(uid):
    data = request.get_json() or {}
    try:
        with get_app_pg() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE app.users SET ai_limit_day=%s, plan=%s WHERE id=%s",
                        [int(data.get("limit",2)), data.get("plan","free"), uid])
            conn.commit(); cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════════════════════
#  TALLY API ROUTES  (all use get_tally_pg)
# ══════════════════════════════════════════════════════════════════════════

@app.route("/api/companies")
@require_auth
def companies():
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
            cur.execute(f"SELECT id,name,last_sync_at,voucher_count,entry_count FROM {S}.companies ORDER BY name")
            rows=[dict(r) for r in cur.fetchall()]; cur.close()
        return jsonify({"companies":rows})
    except Exception as e:
        return jsonify({"error":str(e),"companies":[]}),500

@app.route("/api/dashboard")
@require_auth
def dashboard():
    company=request.args.get("company","").strip()
    if not company:
        try:
            with get_tally_pg() as conn:
                cur=conn.cursor()
                cur.execute(f"SELECT name FROM {S}.companies ORDER BY id LIMIT 1")
                row=cur.fetchone(); cur.close()
                company=row["name"] if row else ""
        except: pass
    if not company: return jsonify({"error":"No company"}),404
    today=date.today().isoformat()
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
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
                WITH pb AS (SELECT ledger_name,SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END) AS nb,MAX(voucher_date::date) AS ltd FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%' AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%' GROUP BY ledger_name HAVING SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END)>0)
                SELECT SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 0 AND 45 THEN nb ELSE 0 END) d0,SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 46 AND 90 THEN nb ELSE 0 END) d1,SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 91 AND 135 THEN nb ELSE 0 END) d2,SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 136 AND 180 THEN nb ELSE 0 END) d3,SUM(CASE WHEN CURRENT_DATE-ltd BETWEEN 181 AND 225 THEN nb ELSE 0 END) d4,SUM(CASE WHEN CURRENT_DATE-ltd > 225 THEN nb ELSE 0 END) d5,SUM(CASE WHEN CURRENT_DATE-ltd > 45 THEN nb ELSE 0 END) overdue,SUM(nb) total,SUM(CASE WHEN CURRENT_DATE-ltd <= 15 THEN nb ELSE 0 END) proj15,SUM(CASE WHEN CURRENT_DATE-ltd <= 60 THEN nb ELSE 0 END) proj60 FROM pb
            """,[company])
            rec=cur.fetchone()
            aging_labels=["0-45 Days","46-90 Days","91-135 Days","136-180 Days","181-225 Days","225+ Days"]
            aging_keys=["d0","d1","d2","d3","d4","d5"]
            aging=[{"label":aging_labels[i],"amount":float(rec[k] or 0),"formatted":fmt(float(rec[k] or 0))} for i,k in enumerate(aging_keys)]
            cur.execute(f"SELECT ledger_name AS name,SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END) AS amount,COUNT(DISTINCT voucher_number) AS bills FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%' AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%' GROUP BY ledger_name ORDER BY amount DESC LIMIT 10",[company])
            top_cust=[{"name":r["name"],"amount":float(r["amount"] or 0),"bills":r["bills"],"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
            cur.execute(f"SELECT ledger_name AS name,SUM(CASE WHEN entry_type='CR' THEN ABS(amount) ELSE -ABS(amount) END) AS amount,COUNT(DISTINCT voucher_number) AS bills FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%purchase%%' AND voucher_type NOT ILIKE '%%order%%' AND ledger_name NOT ILIKE '%%purchase%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%' GROUP BY ledger_name ORDER BY amount DESC LIMIT 10",[company])
            top_sup=[{"name":r["name"],"amount":float(r["amount"] or 0),"bills":r["bills"],"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
            cur.execute(f"SELECT name,closing_balance FROM {S}.ledgers WHERE company_name=%s AND LOWER(parent) IN ('bank accounts','bank od a/c','bank od account') ORDER BY ABS(closing_balance) DESC",[company])
            bank_ledgers=[{"name":r["name"],"balance":float(r["closing_balance"] or 0)} for r in cur.fetchall()]
            cur.execute(f"SELECT COUNT(DISTINCT ledger_name) AS cnt FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%' AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%' AND voucher_date < CURRENT_DATE - INTERVAL '90 days' AND ledger_name NOT IN (SELECT DISTINCT ledger_name FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_date >= CURRENT_DATE - INTERVAL '90 days')",[company,company])
            inactive=int(cur.fetchone()["cnt"] or 0)
            cur.execute(f"SELECT v.voucher_number,v.voucher_type AS type,v.narration,ROUND(SUM(CASE WHEN e.entry_type='DR' THEN ABS(e.amount) ELSE 0 END),2) AS amount,v.voucher_date::TEXT AS date FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name WHERE v.company_name=%s AND v.voucher_date=%s GROUP BY v.voucher_number,v.voucher_type,v.narration,v.voucher_date ORDER BY v.voucher_number",[company,today])
            day_book=[{"voucher":r["voucher_number"],"type":r["type"],"particulars":r["narration"] or "","amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0)),"date":r["date"]} for r in cur.fetchall()]
            net_assets=cash+bank-payables
            cur.close()
        return jsonify({
            "company":company,"today":today,"fy_label":fy_label,
            "sales":{"total":{"amount":ts,"formatted":fmt(ts)},"this_month":{"amount":s_this,"formatted":fmt(s_this),"vs_last":pct(s_this,s_last)},"purchase_total":{"amount":tp,"formatted":fmt(tp)},"purchase_month":{"amount":p_this,"formatted":fmt(p_this),"vs_last":pct(p_this,p_last)},"receipt_total":{"amount":tr,"formatted":fmt(tr)},"payment_total":{"amount":tpy,"formatted":fmt(tpy)},"trend":trend},
            "receivables":{"total":{"amount":float(rec["total"] or 0),"formatted":fmt(float(rec["total"] or 0))},"overdue":{"amount":float(rec["overdue"] or 0),"formatted":fmt(float(rec["overdue"] or 0))},"aging":aging,"proj_15":{"amount":float(rec["proj15"] or 0),"formatted":fmt(float(rec["proj15"] or 0))},"proj_60":{"amount":float(rec["proj60"] or 0),"formatted":fmt(float(rec["proj60"] or 0))}},
            "summary":{"cash":{"amount":cash,"formatted":fmt(cash)},"bank":{"amount":bank,"formatted":fmt(bank)},"payables":{"amount":payables,"formatted":fmt(payables)},"net_assets":{"amount":net_assets,"formatted":fmt(net_assets)}},
            "top_customers":top_cust,"top_suppliers":top_sup,"bank_ledgers":bank_ledgers,
            "inactive_customers":inactive,"day_book":day_book
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error":str(e)}),500

@app.route("/api/sales/detail")
@require_auth
def sales_detail():
    company=request.args.get("company",""); month=request.args.get("month","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",50)); offset=(page-1)*limit
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
            FY_START,FY_END,_=get_data_fy(company,cur)
            q_filter = f"AND TO_CHAR(v.voucher_date,'Mon-YY')=%s" if month else f"AND v.voucher_date>=%s AND v.voucher_date<%s"
            params = [company, month, limit, offset] if month else [company, FY_START, FY_END, limit, offset]
            cur.execute(f"""SELECT v.voucher_number,v.voucher_date::TEXT AS date,v.narration,STRING_AGG(DISTINCT CASE WHEN e.entry_type='DR' AND e.ledger_name NOT ILIKE '%%sale%%' AND e.ledger_name NOT ILIKE '%%gst%%' AND e.ledger_name NOT ILIKE '%%tax%%' THEN e.ledger_name END,', ') AS party,ROUND(SUM(CASE WHEN e.entry_type='DR' THEN ABS(e.amount) ELSE 0 END),2) AS amount FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name WHERE v.company_name=%s AND v.voucher_type ILIKE '%%sale%%' AND v.voucher_type NOT ILIKE '%%order%%' AND v.voucher_type NOT ILIKE '%%credit%%' {q_filter} GROUP BY v.voucher_number,v.voucher_date,v.narration ORDER BY v.voucher_date DESC LIMIT %s OFFSET %s""", params)
            rows=[{"voucher":r["voucher_number"],"date":r["date"],"party":r["party"] or "","narration":r["narration"] or "","amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
            cur.close()
        return jsonify({"vouchers":rows,"page":page,"month":month})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/purchase/detail")
@require_auth
def purchase_detail():
    company=request.args.get("company",""); month=request.args.get("month","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",50)); offset=(page-1)*limit
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
            FY_START,FY_END,_=get_data_fy(company,cur)
            q_filter = f"AND TO_CHAR(v.voucher_date,'Mon-YY')=%s" if month else f"AND v.voucher_date>=%s AND v.voucher_date<%s"
            params = [company, month, limit, offset] if month else [company, FY_START, FY_END, limit, offset]
            cur.execute(f"""SELECT v.voucher_number,v.voucher_date::TEXT AS date,v.narration,STRING_AGG(DISTINCT CASE WHEN e.entry_type='CR' AND e.ledger_name NOT ILIKE '%%purchase%%' AND e.ledger_name NOT ILIKE '%%gst%%' AND e.ledger_name NOT ILIKE '%%tax%%' THEN e.ledger_name END,', ') AS party,ROUND(SUM(CASE WHEN e.entry_type='CR' THEN ABS(e.amount) ELSE 0 END),2) AS amount FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name WHERE v.company_name=%s AND v.voucher_type ILIKE '%%purchase%%' AND v.voucher_type NOT ILIKE '%%order%%' {q_filter} GROUP BY v.voucher_number,v.voucher_date,v.narration ORDER BY v.voucher_date DESC LIMIT %s OFFSET %s""", params)
            rows=[{"voucher":r["voucher_number"],"date":r["date"],"party":r["party"] or "","narration":r["narration"] or "","amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
            cur.close()
        return jsonify({"vouchers":rows,"page":page,"month":month})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/receivables/detail")
@require_auth
def receivables_detail():
    company=request.args.get("company",""); bucket=request.args.get("bucket","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",100)); offset=(page-1)*limit
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
            bmap={"0-45":"BETWEEN 0 AND 45","45-90":"BETWEEN 46 AND 90","90-135":"BETWEEN 91 AND 135","135-180":"BETWEEN 136 AND 180","180-225":"BETWEEN 181 AND 225","225+":"> 225"}
            bsql=f"AND CURRENT_DATE-ltd {bmap[bucket]}" if bucket in bmap else ""
            cur.execute(f"""WITH pb AS (SELECT ledger_name,SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END) AS nb,MAX(voucher_date::date) AS ltd,COUNT(DISTINCT voucher_number) AS bills,ROUND(CASE WHEN SUM(ABS(amount))=0 THEN 0 ELSE SUM(ABS(amount)*(CURRENT_DATE-voucher_date::date))/SUM(ABS(amount)) END,0) AS avg_days FROM {S}.voucher_entries WHERE company_name=%s AND voucher_type ILIKE '%%sale%%' AND voucher_type NOT ILIKE '%%order%%' AND ledger_name NOT ILIKE '%%sale%%' AND ledger_name NOT ILIKE '%%gst%%' AND ledger_name NOT ILIKE '%%tax%%' GROUP BY ledger_name HAVING SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END)>0) SELECT ledger_name AS party,nb AS amount,ltd::TEXT AS last_date,bills,avg_days,(CURRENT_DATE-ltd) AS overdue_days FROM pb WHERE TRUE {bsql} ORDER BY nb DESC LIMIT %s OFFSET %s""",[company,limit,offset])
            rows=[{"party":r["party"],"amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0)),"last_date":r["last_date"],"bills":r["bills"],"avg_days":int(r["avg_days"] or 0),"overdue_days":int(r["overdue_days"] or 0)} for r in cur.fetchall()]
            cur.close()
        return jsonify({"receivables":rows,"bucket":bucket,"page":page})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/voucher/entries")
@require_auth
def voucher_entries():
    company=request.args.get("company",""); voucher=request.args.get("voucher","")
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
            cur.execute(f"SELECT ledger_name,entry_type,amount FROM {S}.voucher_entries WHERE company_name=%s AND voucher_number=%s ORDER BY entry_type",[company,voucher])
            rows=[{"ledger":r["ledger_name"],"type":r["entry_type"],"amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
            cur.close()
        return jsonify({"entries":rows,"voucher":voucher})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/ledger/detail")
@require_auth
def ledger_detail():
    company=request.args.get("company",""); ledger=request.args.get("ledger","")
    page=int(request.args.get("page",1)); limit=int(request.args.get("limit",50)); offset=(page-1)*limit
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
            cur.execute(f"SELECT e.voucher_number,e.voucher_date::TEXT AS date,e.voucher_type AS type,e.entry_type,ABS(e.amount) AS amount FROM {S}.voucher_entries e WHERE e.company_name=%s AND e.ledger_name=%s ORDER BY e.voucher_date DESC, e.voucher_number DESC LIMIT %s OFFSET %s",[company,ledger,limit,offset])
            rows=[{"voucher":r["voucher_number"],"date":r["date"],"type":r["type"],"entry_type":r["entry_type"],"amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0))} for r in cur.fetchall()]
            cur.execute(f"SELECT SUM(CASE WHEN entry_type='DR' THEN ABS(amount) ELSE -ABS(amount) END) AS bal FROM {S}.voucher_entries WHERE company_name=%s AND ledger_name=%s",[company,ledger])
            bal=float(cur.fetchone()["bal"] or 0)
            cur.close()
        return jsonify({"entries":rows,"balance":bal,"formatted":fmt(bal),"ledger":ledger,"page":page})
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/daybook")
@require_auth
def daybook():
    company=request.args.get("company",""); dt=request.args.get("date",date.today().isoformat())
    try:
        with get_tally_pg() as conn:
            cur=conn.cursor()
            cur.execute(f"SELECT v.voucher_number,v.voucher_type AS type,v.narration,ROUND(SUM(CASE WHEN e.entry_type='DR' THEN ABS(e.amount) ELSE 0 END),2) AS amount,v.voucher_date::TEXT AS date FROM {S}.vouchers v JOIN {S}.voucher_entries e ON e.voucher_number=v.voucher_number AND e.company_name=v.company_name WHERE v.company_name=%s AND v.voucher_date=%s GROUP BY v.voucher_number,v.voucher_type,v.narration,v.voucher_date ORDER BY v.voucher_number",[company,dt])
            rows=[{"voucher":r["voucher_number"],"type":r["type"],"particulars":r["narration"] or "","amount":float(r["amount"] or 0),"formatted":fmt(float(r["amount"] or 0)),"date":r["date"]} for r in cur.fetchall()]
            cur.close()
        return jsonify({"entries":rows,"date":dt,"count":len(rows)})
    except Exception as e:
        return jsonify({"error":str(e)}),500

# ── STATIC FILES ───────────────────────────────────────────────────────────
@app.route("/")
def index(): return send_from_directory(".","index.html")
@app.route("/login")
def login_page(): return send_from_directory(".","login.html")
@app.route("/admin")
def admin_page(): return send_from_directory(".","admin.html")
@app.route("/<path:path>")
def static_files(path): return send_from_directory(".",path)

if __name__=="__main__":
    port=int(os.getenv("PORT",8080))
    print(f"\n  BizSage Dashboard v3 → http://localhost:{port}\n")
    app.run(host="0.0.0.0",port=port,debug=False)