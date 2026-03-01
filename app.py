from flask import Flask, render_template, request, redirect, url_for, session, g, Response, stream_with_context, flash
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3
from datetime import datetime, date, timedelta
import queue
import json
import os
import math
import threading
import time
import uuid

# Future scalability notes (high-level):
# - PostgreSQL: replace sqlite3 with SQLAlchemy or psycopg2; add indexes (generated_time, service_name, status, handled_by)
#   and move heavy analytics to materialized views / scheduled jobs.
# - ML demand prediction: replace heuristic peak-hour + wait-time estimation with time-series forecasting (Prophet/LSTM)
#   trained on historical arrivals + service durations.
# - Cloud multi-branch monitoring: add a `branch_id` column and aggregate metrics per branch; central dashboard can read
#   from a shared DB or stream branch telemetry.
# - SMS notifications (Twilio): on token generation or when ETA changes, send SMS updates; store consent + phone securely.

app = Flask(__name__)
limiter = Limiter(get_remote_address, app=app, default_limits=[])
# Use SECRET_KEY env var if provided, otherwise generate a random key at startup.
app.secret_key = os.environ.get('SECRET_KEY') or os.urandom(24)
# Make sessions permanent and set a lifetime for security
app.permanent_session_lifetime = timedelta(minutes=30)
DB_PATH = 'tokens.db'

# SLA thresholds (minutes) for monitoring.
SLA_WAIT_THRESHOLD_MIN = 20
SLA_SERVICE_THRESHOLD_MIN = 15

# Doctor-queue SLA (minutes)
WAITING_SLA = 20

# Department SLA thresholds (minutes) – can be overridden per department
DEFAULT_DEPARTMENT_SLA = 20

# Auto-balance admin setting (display-only vs auto-reassign)
AUTO_BALANCE_ENABLED = False  # Set to True to enable automatic reassignments

FOLLOWUP_MAX_DAYS = 30

FATIGUE_MAX_CONSECUTIVE_PATIENTS = 6
FATIGUE_MAX_CONTINUOUS_MIN = 120
FATIGUE_BREAK_MIN = 15

FAIRNESS_ELEVATE_WAIT_MIN = 45
RISK_REEVAL_INTERVAL_SEC = 120

# Operational heuristics.
CONGESTION_QUEUE_LEN_THRESHOLD = 8
IDLE_THRESHOLD_MIN = 15

# Localization (NFR 4.2)
# English is included by default; additional languages can be added by extending TRANSLATIONS.
DEFAULT_LANG = 'en'
TRANSLATIONS = {
    'en': {
        'stma': 'STMA',
        'login': 'Login',
        'register': 'Register',
        'logout': 'Logout',
        'generate': 'Generate',
        'service_panel': 'Service Panel',
        'dashboard': 'Dashboard',
        'users': 'Users',
        'username': 'Username',
        'password': 'Password',
        'role': 'Role',
        'back_to_login': 'Back to login',
        'generate_token': 'Generate Token',
        'recent_tokens': 'Recent tokens',
        'estimated_waiting_time': 'Estimated Waiting Time',
        'admin_intelligence_dashboard': 'Admin Intelligence Dashboard',
        'service_token_intelligence': 'Service Token Intelligence',
        'today': 'Today',
        'operational_health_score': 'Operational Health Score',
        'hourly_traffic': 'Hourly traffic',
        'total_tokens': 'Total tokens',
        'avg_waiting': 'Avg waiting',
        'avg_service': 'Avg service',
        'sla_violations': 'SLA Violations',
        'waiting': 'Waiting',
        'service': 'Service',
        'service_efficiency': 'Service Efficiency',
        'ranking': 'Ranking',
        'agent_performance': 'Agent Performance',
        'tokens_per_hour': 'Tokens/hour',
        'start': 'Start',
        'end': 'End',
        'status': 'Status',
        'actions': 'Actions',
        'token_generator': 'Token Generator',
        'your_token': 'Your Token',
        'language': 'Language',
        'display_board': 'Display Board',
        'users': 'Users',
    },
    'hi': {
        'stma': 'STMA',
        'login': 'लॉगिन',
        'register': 'रजिस्टर',
        'logout': 'लॉगआउट',
        'generate': 'टोकन बनाएँ',
        'service_panel': 'सेवा पैनल',
        'dashboard': 'डैशबोर्ड',
        'users': 'यूज़र्स',
        'username': 'यूज़रनेम',
        'password': 'पासवर्ड',
        'role': 'भूमिका',
        'back_to_login': 'लॉगिन पर वापस',
        'generate_token': 'टोकन जनरेट करें',
        'recent_tokens': 'हाल के टोकन',
        'estimated_waiting_time': 'अनुमानित प्रतीक्षा समय',
        'admin_intelligence_dashboard': 'एडमिन इंटेलिजेंस डैशबोर्ड',
        'service_token_intelligence': 'सेवा टोकन इंटेलिजेंस',
        'today': 'आज',
        'operational_health_score': 'ऑपरेशनल हेल्थ स्कोर',
        'hourly_traffic': 'घंटेवार ट्रैफिक',
        'total_tokens': 'कुल टोकन',
        'avg_waiting': 'औसत प्रतीक्षा',
        'avg_service': 'औसत सेवा',
        'sla_violations': 'SLA उल्लंघन',
        'waiting': 'प्रतीक्षा',
        'service': 'सेवा',
        'service_efficiency': 'सेवा दक्षता',
        'ranking': 'रैंकिंग',
        'agent_performance': 'एजेंट प्रदर्शन',
        'tokens_per_hour': 'टोकन/घंटा',
        'start': 'शुरू',
        'end': 'समाप्त',
        'status': 'स्थिति',
        'actions': 'एक्शन',
        'token_generator': 'टोकन जनरेटर',
        'your_token': 'आपका टोकन',
        'language': 'भाषा',
        'display_board': 'डिस्प्ले बोर्ड',
        'users': 'यूज़र्स',
    },
    'kn': {
        'stma': 'STMA',
        'login': 'ಲಾಗಿನ್',
        'register': 'ನೋಂದಣಿ',
        'logout': 'ಲಾಗೌಟ್',
        'generate': 'ಟೋಕನ್ ರಚಿಸಿ',
        'service_panel': 'ಸೇವಾ ಪ್ಯಾನೆಲ್',
        'dashboard': 'ಡ್ಯಾಶ್‌ಬೋರ್ಡ್',
        'users': 'ಬಳಕೆದಾರರು',
        'username': 'ಬಳಕೆದಾರ ಹೆಸರು',
        'password': 'ಗುಪ್ತಪದ',
        'role': 'ಪಾತ್ರ',
        'back_to_login': 'ಲಾಗಿನ್‌ಗೆ ಹಿಂತಿರುಗಿ',
        'generate_token': 'ಟೋಕನ್ ಜನರೇಟ್ ಮಾಡಿ',
        'recent_tokens': 'ಇತ್ತೀಚಿನ ಟೋಕನ್‌ಗಳು',
        'estimated_waiting_time': 'ಅಂದಾಜು ನಿರೀಕ್ಷಣಾ ಸಮಯ',
        'admin_intelligence_dashboard': 'ಆಡ್ಮಿನ್ ಇಂಟೆಲಿಜೆನ್ಸ್ ಡ್ಯಾಶ್‌ಬೋರ್ಡ್',
        'service_token_intelligence': 'ಸೇವಾ ಟೋಕನ್ ಇಂಟೆಲಿಜೆನ್ಸ್',
        'today': 'ಇಂದು',
        'operational_health_score': 'ಕಾರ್ಯಾಚರಣಾ ಆರೋಗ್ಯ ಸ್ಕೋರ್',
        'hourly_traffic': 'ಗಂಟೆವಾರು ಟ್ರಾಫಿಕ್',
        'total_tokens': 'ಒಟ್ಟು ಟೋಕನ್‌ಗಳು',
        'avg_waiting': 'ಸರಾಸರಿ ನಿರೀಕ್ಷೆ',
        'avg_service': 'ಸರಾಸರಿ ಸೇವೆ',
        'sla_violations': 'SLA ಉಲ್ಲಂಘನೆಗಳು',
        'waiting': 'ನಿರೀಕ್ಷೆ',
        'service': 'ಸೇವೆ',
        'service_efficiency': 'ಸೇವಾ ಕಾರ್ಯಕ್ಷಮತೆ',
        'ranking': 'ರ್ಯಾಂಕಿಂಗ್',
        'agent_performance': 'ಏಜೆಂಟ್ ಕಾರ್ಯಕ್ಷಮತೆ',
        'tokens_per_hour': 'ಟೋಕನ್‌ಗಳು/ಗಂಟೆ',
        'start': 'ಪ್ರಾರಂಭ',
        'end': 'ಮುಗಿಸಿ',
        'status': 'ಸ್ಥಿತಿ',
        'actions': 'ಕ್ರಿಯೆಗಳು',
        'token_generator': 'ಟೋಕನ್ ಜನರೇಟರ್',
        'your_token': 'ನಿಮ್ಮ ಟೋಕನ್',
        'language': 'ಭಾಷೆ',
        'display_board': 'ಡಿಸ್ಪ್ಲೇ ಬೋರ್ಡ್',
        'users': 'ಬಳಕೆದಾರರು',
    },
    'te': {
        'stma': 'STMA',
        'login': 'లాగిన్',
        'register': 'నమోదు',
        'logout': 'లాగౌట్',
        'generate': 'టోకెన్ సృష్టించండి',
        'service_panel': 'సేవా ప్యానెల్',
        'dashboard': 'డాష్‌బోర్డ్',
        'users': 'వినియోగదారులు',
        'username': 'వినియోగదారు పేరు',
        'password': 'పాస్‌వర్డ్',
        'role': 'పాత్ర',
        'back_to_login': 'లాగిన్‌కు తిరిగి',
        'generate_token': 'టోకెన్ జనరేట్ చేయండి',
        'recent_tokens': 'ఇటీవలి టోకెన్లు',
        'estimated_waiting_time': 'అంచనా వేచిచూడే సమయం',
        'admin_intelligence_dashboard': 'అడ్మిన్ ఇంటెలిజెన్స్ డాష్‌బోర్డ్',
        'service_token_intelligence': 'సేవా టోకెన్ ఇంటెలిజెన్స్',
        'today': 'ఈ రోజు',
        'operational_health_score': 'ఆపరేషనల్ హెల్త్ స్కోర్',
        'hourly_traffic': 'గంటల వారీ ట్రాఫిక్',
        'total_tokens': 'మొత్తం టోకెన్లు',
        'avg_waiting': 'సగటు వేచి',
        'avg_service': 'సగటు సేవ',
        'sla_violations': 'SLA ఉల్లంఘనలు',
        'waiting': 'వేచి',
        'service': 'సేవ',
        'service_efficiency': 'సేవా సామర్థ్యం',
        'ranking': 'ర్యాంకింగ్',
        'agent_performance': 'ఏజెంట్ పనితీరు',
        'tokens_per_hour': 'టోకెన్లు/గంట',
        'start': 'ప్రారంభించు',
        'end': 'ముగించు',
        'status': 'స్థితి',
        'actions': 'చర్యలు',
        'token_generator': 'టోకెన్ జనరేటర్',
        'your_token': 'మీ టోకెన్',
        'language': 'భాష',
        'display_board': 'డిస్ప్లే బోర్డు',
        'users': 'వినియోగదారులు',
    },
}


def get_lang():
    lang = session.get('lang')
    if lang in TRANSLATIONS:
        return lang
    return DEFAULT_LANG


def _(key: str) -> str:
    lang = get_lang()
    return TRANSLATIONS.get(lang, TRANSLATIONS[DEFAULT_LANG]).get(key, key)


def calculate_average_service_time(service_name: str | None = None):
    """Calculate average service time (minutes) from completed tokens.

    This is a simple heuristic baseline.
    In production, you can replace this with an ML-based predictor that considers:
    - time-of-day demand
    - staffing levels
    - service mix / patient complexity
    and outputs confidence intervals.
    """
    if service_name:
        rows = query_db(
            """
            SELECT start_time, end_time
            FROM tokens
            WHERE status='Completed'
              AND service_name=?
              AND start_time IS NOT NULL
              AND end_time IS NOT NULL
            """,
            (service_name,),
        )
    else:
        rows = query_db(
            """
            SELECT start_time, end_time
            FROM tokens
            WHERE status='Completed'
              AND start_time IS NOT NULL
              AND end_time IS NOT NULL
            """
        )

    mins = []
    for r in rows:
        s = _parse_iso(r['start_time'])
        e = _parse_iso(r['end_time'])
        if s and e and e >= s:
            mins.append(_minutes(e - s))
    if not mins:
        return None
    return round(sum(mins) / len(mins), 2)


def get_emergency_doctors():
    try:
        return query_db(
            """
            SELECT id, name
            FROM doctors
            WHERE is_active=1
              AND COALESCE(is_emergency_doctor, 0)=1
            ORDER BY name
            """
        )
    except sqlite3.OperationalError:
        # Backward compatibility if column missing.
        return []


def _pick_available_emergency_doctor():
    docs = get_emergency_doctors()
    if not docs:
        return None
    for d in docs:
        did = int(d['id'])
        busy = query_db(
            """
            SELECT id
            FROM emergency_cases
            WHERE status='ACTIVE'
              AND assigned_doctor_id=?
            LIMIT 1
            """,
            (did,),
            one=True,
        )
        if not busy:
            return did
    return None


def _emergency_overload_active() -> bool:
    row = query_db(
        """
        SELECT COUNT(*) AS cnt
        FROM emergency_cases
        WHERE status='ACTIVE'
          AND assigned_doctor_id IS NULL
        """,
        one=True,
    )
    try:
        return int(row['cnt'] or 0) > 0
    except Exception:
        return False


def _get_emergency_state():
    row = query_db("SELECT * FROM emergency_state WHERE id=1", one=True)
    if not row:
        return {'surge_active': 0, 'surge_doctor_id': None, 'activated_at': None}
    return {
        'surge_active': int(row['surge_active'] or 0),
        'surge_doctor_id': int(row['surge_doctor_id']) if row['surge_doctor_id'] is not None else None,
        'activated_at': row['activated_at'],
    }


def _set_emergency_state(surge_active: int, surge_doctor_id: int | None, activated_at: str | None):
    db = get_db()
    db.execute(
        "UPDATE emergency_state SET surge_active=?, surge_doctor_id=?, activated_at=? WHERE id=1",
        (int(surge_active or 0), surge_doctor_id, activated_at),
    )
    db.commit()


def calculate_estimated_wait_time(service_name: str, generated_time_iso: str):
    """Estimated waiting time (minutes).

    EstimatedWaitTime = AvgServiceTime(service) × (tokens ahead in queue)

    Tokens ahead are those with status "Generated" or "Waiting".
    """
    avg_service = calculate_average_service_time(service_name)
    if avg_service is None:
        return None

    ahead_row = query_db(
        """
        SELECT COUNT(*) AS cnt
        FROM tokens
        WHERE service_name=?
          AND status IN ('Generated','Waiting')
          AND generated_time < ?
        """,
        (service_name, generated_time_iso),
        one=True,
    )
    tokens_ahead = int(ahead_row['cnt']) if ahead_row else 0
    return round(avg_service * tokens_ahead, 1)


def check_sla_status(avg_wait_min: float | None, avg_service_min: float | None, queue_len: int | None = None):
    """Rule-based decision support (not ML).

    If average waiting time breaches SLA, flag breach and provide a recommendation.
    """
    breach = False
    # If queue length is known and currently empty, treat as compliant to avoid confusing public display.
    if queue_len is not None and queue_len <= 0:
        breach = False
    elif avg_wait_min is not None and avg_wait_min > SLA_WAIT_THRESHOLD_MIN:
        breach = True

    if breach:
        return {
            'breach': True,
            'title': 'SLA Breach Detected.',
            'recommendation': 'Recommendation: Allocate one additional service counter during peak hours.',
        }

    return {
        'breach': False,
        'title': 'All services operating within SLA compliance.',
        'recommendation': None,
    }


def _compute_sla_breached_for_token_row(r: dict) -> int:
    """Compute SLA breach for a token row using existing thresholds.

    Note: Emergency tokens are NOT penalized (returns 0) to avoid bias.
    """
    try:
        if _safe_patient_category(r.get('patient_category')) == 'Emergency':
            return 0
    except Exception:
        pass

    gen = _parse_iso(r.get('generated_time'))
    assigned = _parse_iso(r.get('assigned_time'))
    start = _parse_iso(r.get('start_time'))
    end = _parse_iso(r.get('end_time'))

    ref = assigned or gen
    wait_breach = 0
    if ref and start and start >= ref:
        w = _minutes(start - ref)
        if w is not None and w > SLA_WAIT_THRESHOLD_MIN:
            wait_breach = 1
    svc_breach = 0
    if start and end and end >= start:
        s = _minutes(end - start)
        if s is not None and s > SLA_SERVICE_THRESHOLD_MIN:
            svc_breach = 1
    return 1 if (wait_breach or svc_breach) else 0


@app.context_processor
def inject_i18n():
    return {
        '_': _,
        'current_lang': get_lang(),
        'available_langs': sorted(list(TRANSLATIONS.keys())),
    }


@app.route('/set_language', methods=['POST'])
def set_language():
    lang = (request.form.get('lang') or '').strip()
    if lang in TRANSLATIONS:
        session['lang'] = lang
    ref = request.referrer
    return redirect(ref or url_for('index'))


def get_db():
    """Open a connection to the SQLite database and return the connection.
    We use `sqlite3.Row` so rows behave like dicts for templates.
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def init_db():
    """Create database and tables if they do not exist.

    Tables:
    - users: stores application users (id, username, password_hash, role)
      role can be 'Admin', 'Receptionist', or 'Service'.

    - tokens: stores generated service tokens and timestamps
      (id, token_number, service_name, generated_time, start_time, end_time, status)
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Users table: stores users for login and role-based access
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS patients (
            id TEXT PRIMARY KEY,
            user_id INTEGER UNIQUE,
            full_name TEXT,
            phone_number TEXT,
            created_at TEXT NOT NULL
        )
    ''')

    # If the database has older/extra columns from email verification,
    # rebuild the users table to remove: email, is_verified, verification_token.
    c.execute("PRAGMA table_info(users)")
    user_cols = [r[1] for r in c.fetchall()]
    extra_cols = {'email', 'is_verified', 'verification_token'}
    if any(col in user_cols for col in extra_cols):
        c.execute('''
            CREATE TABLE IF NOT EXISTS users_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL
            )
        ''')
        c.execute('''
            INSERT INTO users_new (id, username, password, role)
            SELECT id, username, password, role
            FROM users
        ''')
        c.execute('DROP TABLE users')
        c.execute('ALTER TABLE users_new RENAME TO users')
    # Tokens table: stores tokens and timestamps
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_number TEXT NOT NULL,
            service_name TEXT NOT NULL,
            generated_time TEXT NOT NULL,
            assigned_time TEXT,
            doctor_id INTEGER,
            created_at TEXT,
            called_at TEXT,
            closed_at TEXT,
            service_duration REAL,
            sla_breached INTEGER NOT NULL DEFAULT 0,
            is_followup INTEGER NOT NULL DEFAULT 0,
            followup_of_token_id INTEGER,
            priority_level INTEGER NOT NULL DEFAULT 3,
            priority TEXT DEFAULT 'normal',
            patient_category TEXT NOT NULL DEFAULT 'Regular',
            risk_score INTEGER NOT NULL DEFAULT 30,
            ai_priority_level INTEGER NOT NULL DEFAULT 4,
            created_by_role TEXT NOT NULL DEFAULT 'STAFF',
            created_by_user_id INTEGER,
            start_time TEXT,
            end_time TEXT,
            handled_by TEXT,
            status TEXT NOT NULL
        )
    ''')

    # Doctors table: doctor-wise queue allocation.
    c.execute('''
        CREATE TABLE IF NOT EXISTS doctors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            specialization TEXT,
            department_id INTEGER,
            is_active INTEGER NOT NULL DEFAULT 1
        )
    ''')

    # Emergency cases are a separate workflow from OPD tokens.
    c.execute('''
        CREATE TABLE IF NOT EXISTS emergency_cases (
            id TEXT PRIMARY KEY,
            patient_name TEXT NOT NULL,
            triage_level INTEGER NOT NULL,
            assigned_doctor_id INTEGER,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            closed_at TEXT,
            created_by_user_id INTEGER,
            notes TEXT
        )
    ''')

    # Emergency workflow state (single-row table): surge mode tracking.
    c.execute('''
        CREATE TABLE IF NOT EXISTS emergency_state (
            id INTEGER PRIMARY KEY,
            surge_active INTEGER NOT NULL DEFAULT 0,
            surge_doctor_id INTEGER,
            activated_at TEXT
        )
    ''')
    try:
        c.execute("INSERT OR IGNORE INTO emergency_state (id, surge_active) VALUES (1, 0)")
    except Exception:
        pass

    # Departments table: SLA thresholds per department.
    c.execute('''
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            sla_threshold_minutes INTEGER NOT NULL DEFAULT 20
        )
    ''')

    # Audit logs for reassignments (manager actions and auto-balance events).
    c.execute('''
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS queue_health_scores (
            day TEXT PRIMARY KEY,
            score REAL NOT NULL,
            created_at TEXT NOT NULL
        )
    ''')

    # Anomaly log: persistent audit trail for SLA violations / unusual queue events.
    c.execute('''
        CREATE TABLE IF NOT EXISTS anomaly_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id INTEGER,
            token_number TEXT,
            service_name TEXT,
            handled_by TEXT,
            anomaly_type TEXT NOT NULL,
            observed_value REAL,
            threshold_value REAL,
            created_at TEXT NOT NULL,
            UNIQUE(token_id, anomaly_type)
        )
    ''')
    conn.commit()
    # If an older tokens table exists with column `service` (from prior versions),
    # add `service_name` column and copy values so the rest of the app works.
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tokens'")
    if c.fetchone():
        c.execute("PRAGMA table_info(tokens)")
        cols = [r[1] for r in c.fetchall()]
        if 'service_name' not in cols and 'service' in cols:
            # add new column and copy data from old `service` column
            c.execute("ALTER TABLE tokens ADD COLUMN service_name TEXT")
            c.execute("UPDATE tokens SET service_name = service")
            conn.commit()

        if 'handled_by' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN handled_by TEXT")
            conn.commit()

        # Doctor-based queue support
        if 'doctor_id' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN doctor_id INTEGER")
            conn.commit()
        if 'assigned_time' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN assigned_time TEXT")
            conn.commit()
        if 'created_at' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN created_at TEXT")
            conn.commit()
        if 'called_at' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN called_at TEXT")
            conn.commit()
        if 'closed_at' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN closed_at TEXT")
            conn.commit()
        if 'service_duration' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN service_duration REAL")
            conn.commit()
        if 'sla_breached' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN sla_breached INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        if 'priority' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN priority TEXT DEFAULT 'normal'")
            conn.commit()

        if 'is_followup' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN is_followup INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        if 'followup_of_token_id' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN followup_of_token_id INTEGER")
            conn.commit()
        if 'priority_level' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN priority_level INTEGER NOT NULL DEFAULT 3")
            conn.commit()

        if 'patient_category' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN patient_category TEXT NOT NULL DEFAULT 'Regular'")
            conn.commit()
        if 'risk_score' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN risk_score INTEGER NOT NULL DEFAULT 30")
            conn.commit()
        if 'ai_priority_level' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN ai_priority_level INTEGER NOT NULL DEFAULT 4")
            conn.commit()

        if 'patient_name' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN patient_name TEXT")
            conn.commit()

        if 'created_by_role' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN created_by_role TEXT NOT NULL DEFAULT 'STAFF'")
            conn.commit()
        if 'created_by_user_id' not in cols:
            c.execute("ALTER TABLE tokens ADD COLUMN created_by_user_id INTEGER")
            conn.commit()

        # Indexes for priority ordering
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_tokens_priority_level ON tokens(priority_level)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tokens_ai_priority_level ON tokens(ai_priority_level)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tokens_patient_category ON tokens(patient_category)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tokens_followup_of ON tokens(followup_of_token_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tokens_handled_by ON tokens(handled_by)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_tokens_sla_breached ON tokens(sla_breached)")
            conn.commit()
        except Exception:
            pass

        # Backfill lifecycle fields best-effort.
        try:
            c.execute("UPDATE tokens SET created_at = COALESCE(created_at, generated_time)")
            c.execute("UPDATE tokens SET called_at = COALESCE(called_at, start_time)")
            c.execute("UPDATE tokens SET closed_at = COALESCE(closed_at, end_time)")
            # service_duration in minutes when possible
            c.execute(
                """
                UPDATE tokens
                SET service_duration = (
                    (julianday(end_time) - julianday(start_time)) * 24.0 * 60.0
                )
                WHERE service_duration IS NULL
                  AND start_time IS NOT NULL
                  AND end_time IS NOT NULL
                """
            )
            conn.commit()
        except Exception:
            pass
    conn.close()

    # Add department_id to doctors table if missing (migration)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('PRAGMA table_info(doctors)')
    doctor_cols = [row[1] for row in c.fetchall()]
    if 'department_id' not in doctor_cols:
        c.execute("ALTER TABLE doctors ADD COLUMN department_id INTEGER")
        conn.commit()

    # Add is_emergency_doctor to doctors table if missing.
    if 'is_emergency_doctor' not in doctor_cols:
        try:
            c.execute("ALTER TABLE doctors ADD COLUMN is_emergency_doctor INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        except Exception:
            pass
    # Backfill doctors.department_id for existing rows (best-effort)
    try:
        c.execute("SELECT id, name FROM departments")
        dept_rows = c.fetchall()
        dept_map = {name: did for (did, name) in dept_rows}
        # Map known specializations to departments.
        # If your 'specialization' values match department names, this will also work.
        c.execute("SELECT id, specialization FROM doctors WHERE department_id IS NULL")
        for doctor_id, specialization in c.fetchall():
            if not specialization:
                continue
            did = dept_map.get(specialization)
            if did:
                c.execute("UPDATE doctors SET department_id=? WHERE id=?", (did, doctor_id))
        conn.commit()
    except Exception:
        # Don't block app startup if backfill fails for some reason.
        pass
    conn.close()

    # Seed departments and doctors (beginner-friendly defaults)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM departments')
    if c.fetchone()[0] == 0:
        c.executemany(
            'INSERT INTO departments (name, sla_threshold_minutes) VALUES (?,?)',
            [
                ('Consultation', 20),
                ('Lab Test', 15),
                ('X-Ray', 10),
                ('ENT', 20),
                ('Skin Treatment', 20),
                ('Ear Checkup', 20),
                ('Back Pain', 20),
            ],
        )
    else:
        c.execute('SELECT name FROM departments')
        existing = {r[0] for r in c.fetchall()}
        to_add = [
            ('ENT', 20),
            ('Skin Treatment', 20),
            ('Ear Checkup', 20),
            ('Back Pain', 20),
        ]
        for name, sla in to_add:
            if name not in existing:
                c.execute('INSERT INTO departments (name, sla_threshold_minutes) VALUES (?,?)', (name, sla))
    conn.commit()

    # Map department names to IDs for doctor seeding (use raw connection to avoid app context)
    c = conn.cursor()
    c.execute("SELECT id, name FROM departments")
    dept_map = {row[1]: row[0] for row in c.fetchall()}
    def ensure_doctor(name, specialization, department_name, is_active=1):
        c.execute('SELECT id FROM doctors WHERE name=?', (name,))
        if c.fetchone():
            return
        c.execute(
            'INSERT INTO doctors (name, specialization, department_id, is_active) VALUES (?,?,?,?)',
            (name, specialization, dept_map.get(department_name), is_active),
        )

    # Keep exactly 4 active doctors (simple starter set)
    active_doctors = [
        ('Dr. Asha', 'Consultation', 'Consultation'),
        ('Dr. Kiran', 'Consultation', 'Consultation'),
        ('Dr. Meera', 'Lab Test', 'Lab Test'),
        ('Dr. Anil', 'X-Ray', 'X-Ray'),
        ('Dr. ENT-1', 'ENT', 'ENT'),
        ('Dr. Skin-1', 'Skin Treatment', 'Skin Treatment'),
        ('Dr. Ear-1', 'Ear Checkup', 'Ear Checkup'),
        ('Dr. Ortho-1', 'Back Pain', 'Back Pain'),
    ]

    for name, specialization, dept_name in active_doctors:
        ensure_doctor(name, specialization, dept_name, 1)
        # ensure they are active if they already existed
        c.execute('UPDATE doctors SET is_active=1 WHERE name=?', (name,))

    # Ensure at least one emergency doctor exists for demo.
    try:
        c.execute("SELECT COUNT(*) FROM doctors WHERE COALESCE(is_emergency_doctor,0)=1")
        em_cnt = int(c.fetchone()[0] or 0)
        if em_cnt <= 0:
            c.execute("UPDATE doctors SET is_emergency_doctor=1 WHERE name=?", ('Dr. Asha',))
    except Exception:
        pass
    conn.commit()
    conn.close()

    # Insert sample users if they don't exist (for quick testing)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    def ensure_user(username, password, role):
        c.execute('SELECT id FROM users WHERE username=?', (username,))
        if not c.fetchone():
            pw = _bcrypt_hash_password(password)
            c.execute('INSERT INTO users (username, password, role) VALUES (?,?,?)', (username, pw, role))
    ensure_user('admin', 'password', 'Admin')
    ensure_user('reception', 'password', 'Receptionist')
    ensure_user('service', 'password', 'Service')
    ensure_user('manager', 'password', 'Manager')
    conn.commit()
    conn.close()

# Simple in-memory broadcaster for Server-Sent Events (SSE)
clients = set()


def register_client():
    q = queue.Queue()
    clients.add(q)
    return q


def unregister_client(q):
    try:
        clients.discard(q)
    except Exception:
        pass


def push_event(event: dict):
    data = json.dumps(event)
    dead = []
    for q in list(clients):
        try:
            q.put_nowait(data)
        except Exception:
            dead.append(q)
    for d in dead:
        unregister_client(d)


@app.route('/stream')
def stream():
    q = register_client()

    def event_stream():
        try:
            while True:
                data = q.get()
                yield f"data: {data}\n\n"
        finally:
            unregister_client(q)

    return Response(stream_with_context(event_stream()), mimetype='text/event-stream')


def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


def get_active_doctors(specialization: str | None = None):
    if specialization:
        return query_db(
            """
            SELECT id, name, specialization
            FROM doctors
            WHERE is_active=1
              AND (specialization=? OR specialization IS NULL OR specialization='')
            ORDER BY name
            """,
            (specialization,),
        )
    return query_db(
        """
        SELECT id, name, specialization
        FROM doctors
        WHERE is_active=1
        ORDER BY name
        """
    )


def get_doctor_queue_length(doctor_id: int):
    row = query_db(
        """
        SELECT COUNT(*) AS cnt
        FROM tokens
        WHERE doctor_id=?
          AND status IN ('Waiting','Generated','InProgress')
        """,
        (doctor_id,),
        one=True,
    )
    return int(row['cnt']) if row else 0


def _safe_patient_category(category: str | None) -> str:
    c = (category or '').strip()
    allowed = {'Senior Citizen', 'Chronic Illness', 'Emergency', 'Regular'}
    return c if c in allowed else 'Regular'


def _ai_priority_from_risk(risk_score: int) -> int:
    r = int(risk_score)
    if r >= 80:
        return 1
    if r >= 60:
        return 2
    if r >= 40:
        return 3
    return 4


def _base_risk_for_category(category: str) -> int:
    c = (category or '').strip().lower()
    if c == 'emergency':
        return 100
    if c == 'chronic illness' or c == 'chronic':
        return 75
    if c == 'senior citizen' or c == 'senior':
        return 60
    return 30


def _risk_reason_parts(category: str, is_followup: int, wait_over_sla: bool, doctor_overloaded: bool, fairness_elevated: bool):
    parts = []
    if category:
        parts.append(f"Category: {category}")
    if is_followup:
        parts.append("Follow-up")
    if wait_over_sla:
        parts.append("Waiting > SLA")
    if doctor_overloaded:
        parts.append("Doctor overloaded")
    if fairness_elevated:
        parts.append("Fairness boost")
    return parts


def compute_risk_and_priority(token_row: dict, now_dt: datetime | None = None):
    now_dt = now_dt or datetime.now()
    category = (token_row.get('patient_category') or 'Regular').strip()
    is_followup = int(token_row.get('is_followup') or 0)
    doctor_id = token_row.get('doctor_id')
    service_name = token_row.get('service_name')

    base = _base_risk_for_category(category)

    assigned_time = token_row.get('assigned_time') or token_row.get('generated_time')
    assigned_dt = None
    try:
        assigned_dt = datetime.fromisoformat(assigned_time) if assigned_time else None
    except Exception:
        assigned_dt = None

    wait_min = 0
    if assigned_dt:
        wait_min = max(0, int(_minutes(now_dt - assigned_dt)))

    sla_min = SLA_WAIT_THRESHOLD_MIN
    try:
        dept_row = query_db("SELECT d.sla_threshold_minutes FROM departments d WHERE d.name=?", (service_name,), one=True)
        if dept_row and dept_row['sla_threshold_minutes']:
            sla_min = int(dept_row['sla_threshold_minutes'])
    except Exception:
        pass

    wait_over_sla = wait_min > int(sla_min)
    doctor_overloaded = False
    try:
        if doctor_id:
            doctor_overloaded = bool(doctor_overload_status(int(doctor_id)).get('overload'))
    except Exception:
        doctor_overloaded = False

    risk = base
    if wait_over_sla:
        risk += 10
    if is_followup:
        risk += 10
    if doctor_overloaded:
        risk += 5

    fairness_elevated = False
    if (category or '').strip().lower() in ('regular', '') and wait_min >= FAIRNESS_ELEVATE_WAIT_MIN:
        # Raise regular patients to at least Priority 2 band (>=60) if they wait too long.
        if risk < 60:
            fairness_elevated = True
            risk = 60

    risk = max(0, min(100, int(risk)))
    ai_pri = _ai_priority_from_risk(risk)
    parts = _risk_reason_parts(category, is_followup, wait_over_sla, doctor_overloaded, fairness_elevated)
    return {
        'risk_score': risk,
        'ai_priority_level': ai_pri,
        'reason': '; '.join(parts) if parts else None,
        'wait_min': wait_min,
        'sla_min': sla_min,
    }


def _update_waiting_tokens_risk():
    now_dt = datetime.now()
    rows = query_db(
        """
        SELECT id, service_name, generated_time, assigned_time, doctor_id, is_followup, patient_category, risk_score, ai_priority_level
        FROM tokens
        WHERE status IN ('Waiting','Generated')
        """
    )
    if not rows:
        return 0
    db = get_db()
    updated = 0
    for r in rows:
        calc = compute_risk_and_priority(dict(r), now_dt=now_dt)
        new_risk = int(calc['risk_score'])
        new_ai = int(calc['ai_priority_level'])
        old_risk = int(r['risk_score']) if 'risk_score' in r.keys() and r['risk_score'] is not None else None
        old_ai = int(r['ai_priority_level']) if 'ai_priority_level' in r.keys() and r['ai_priority_level'] is not None else None
        if old_risk != new_risk or old_ai != new_ai:
            db.execute(
                "UPDATE tokens SET risk_score=?, ai_priority_level=? WHERE id=?",
                (new_risk, new_ai, int(r['id'])),
            )
            updated += 1
    if updated:
        db.commit()
        push_event({'type': 'risk_updated', 'updated': updated})
    return updated


def _risk_reeval_loop():
    while True:
        try:
            with app.app_context():
                _update_waiting_tokens_risk()
        except Exception:
            pass
        time.sleep(RISK_REEVAL_INTERVAL_SEC)


def _start_risk_reeval_thread_once():
    global _risk_thread_started
    try:
        if _risk_thread_started:
            return None
    except Exception:
        _risk_thread_started = False

    # Flask debug reloader starts the app twice. Only run the background loop in the reloader child.
    if app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        return None

    _risk_thread_started = True
    t = threading.Thread(target=_risk_reeval_loop, daemon=True)
    t.start()
    return t


def calculate_doctor_avg_service_time(doctor_id: int):
    rows = query_db(
        """
        SELECT start_time, end_time
        FROM tokens
        WHERE doctor_id=?
          AND status='Completed'
          AND start_time IS NOT NULL
          AND end_time IS NOT NULL
        """,
        (doctor_id,),
    )
    mins = []
    for r in rows:
        s = _parse_iso(r['start_time'])
        e = _parse_iso(r['end_time'])
        if s and e and e >= s:
            mins.append(_minutes(e - s))
    if mins:
        return sum(mins) / len(mins)
    return None


def calculate_doctor_avg_waiting_time(doctor_id: int):
    rows = query_db(
        """
        SELECT assigned_time, start_time
        FROM tokens
        WHERE doctor_id=?
          AND assigned_time IS NOT NULL
          AND start_time IS NOT NULL
        """,
        (doctor_id,),
    )
    mins = []
    for r in rows:
        a = _parse_iso(r['assigned_time'])
        s = _parse_iso(r['start_time'])
        if a and s and s >= a:
            mins.append(_minutes(s - a))
    if mins:
        return sum(mins) / len(mins)
    return None


def calculate_doctor_estimated_wait_time(doctor_id: int):
    # tokens ahead in queue for that doctor
    row = query_db(
        """
        SELECT COUNT(*) AS cnt
        FROM tokens
        WHERE doctor_id=?
          AND status IN ('Waiting','Generated')
        """,
        (doctor_id,),
        one=True,
    )
    ahead = int(row['cnt']) if row else 0
    avg_service = calculate_doctor_avg_service_time(doctor_id)
    if avg_service is None:
        return None
    return round(avg_service * ahead, 1)


def emergency_reallocation_recommendation(breaching_doctor_id: int):
    """Display-only recommendation; never auto-reassign tokens.

    Production note:
    - This can be automated using rule-based optimization or AI-based optimization.
    """
    active = get_active_doctors()
    others = [d for d in active if int(d['id']) != int(breaching_doctor_id)]
    if not others:
        return None
    loads = [(d, get_doctor_queue_length(int(d['id']))) for d in others]
    lowest = min(loads, key=lambda x: x[1])
    doctor = lowest[0]
    return f"Emergency Recommendation: Reassign next tokens to Dr. {doctor['name']} to reduce congestion."


def _doctor_name_map():
    rows = query_db("SELECT id, name FROM doctors")
    return {int(r['id']): r['name'] for r in rows}


def get_departments():
    return query_db("SELECT id, name, sla_threshold_minutes FROM departments ORDER BY name")


def get_doctors_by_department(dept_id: int):
    try:
        return query_db(
            "SELECT id, name, specialization, is_active FROM doctors WHERE department_id=? AND is_active=1 ORDER BY name",
            (dept_id,),
        )
    except sqlite3.OperationalError:
        # Backward compatibility: older DBs may not have doctors.department_id yet.
        dept = query_db("SELECT name FROM departments WHERE id=?", (dept_id,), one=True)
        if not dept:
            return []
        # Fallback heuristic: use department name as the doctor's specialization.
        return query_db(
            "SELECT id, name, specialization, is_active FROM doctors WHERE specialization=? AND is_active=1 ORDER BY name",
            (dept['name'],),
        )


def department_sla_status(dept_id: int):
    """Calculate EstimatedWait = (QueueLength × AvgServiceTime) / ActiveDoctors for a department."""
    doctors = get_doctors_by_department(dept_id)
    if not doctors:
        return {'breach': False, 'estimated_wait_min': None, 'sla_min': DEFAULT_DEPARTMENT_SLA}
    total_queue = sum(get_doctor_queue_length(int(d['id'])) for d in doctors)
    active_count = len(doctors)
    # Use overall avg service time as fallback; per-department can be added later
    avg_service = calculate_average_service_time(None)
    if avg_service is None or active_count == 0:
        return {'breach': False, 'estimated_wait_min': None, 'sla_min': DEFAULT_DEPARTMENT_SLA}
    estimated_wait = (total_queue * avg_service) / active_count
    dept_row = query_db("SELECT sla_threshold_minutes FROM departments WHERE id=?", (dept_id,), one=True)
    sla_min = dept_row['sla_threshold_minutes'] if dept_row else DEFAULT_DEPARTMENT_SLA
    return {
        'breach': estimated_wait > sla_min,
        'estimated_wait_min': round(estimated_wait, 2),
        'sla_min': sla_min,
    }


def _clamp(x: float, lo: float, hi: float):
    return max(lo, min(hi, x))


def compute_queue_health_score(*, sla_breach: bool, total_today: int, avg_wait: float | None, sla_wait_min: int, doctor_queue_lens: list[int], overload_flags: list[bool]):
    sla_compliance = 0.0
    if total_today > 0:
        sla_compliance = 0.0 if sla_breach else 1.0
    wait_ratio = 1.0
    if avg_wait is not None and sla_wait_min:
        wait_ratio = _clamp(avg_wait / float(sla_wait_min), 0.0, 3.0)
    wait_component = _clamp(1.0 - (wait_ratio - 1.0), 0.0, 1.0)

    balance_component = 1.0
    if doctor_queue_lens:
        mean = sum(doctor_queue_lens) / len(doctor_queue_lens)
        if mean > 0:
            mad = sum(abs(x - mean) for x in doctor_queue_lens) / len(doctor_queue_lens)
            balance_component = _clamp(1.0 - (mad / (mean + 1e-6)), 0.0, 1.0)

    overload_component = 1.0
    if overload_flags:
        overload_rate = sum(1 for f in overload_flags if f) / len(overload_flags)
        overload_component = _clamp(1.0 - overload_rate, 0.0, 1.0)

    cancellation_component = 1.0

    score = (
        (sla_compliance * 0.40) +
        (wait_component * 0.20) +
        (balance_component * 0.20) +
        (overload_component * 0.10) +
        (cancellation_component * 0.10)
    )
    return int(round(_clamp(score, 0.0, 1.0) * 100.0))


def _get_health_trend(days: int = 7):
    rows = query_db(
        "SELECT day, score FROM queue_health_scores ORDER BY day DESC LIMIT ?",
        (days,),
    )
    out = [{'day': r['day'], 'score': int(round(r['score']))} for r in rows]
    out.reverse()
    return out


def _upsert_today_health_score(score: int):
    day = date.today().isoformat()
    db = get_db()
    db.execute(
        "INSERT OR REPLACE INTO queue_health_scores (day, score, created_at) VALUES (?,?,?)",
        (day, float(score), datetime.now().isoformat()),
    )
    db.commit()


def _fatigue_for_doctor(doctor_id: int):
    tokens = query_db(
        """
        SELECT id, start_time, end_time
        FROM tokens
        WHERE doctor_id=? AND status='Completed' AND start_time IS NOT NULL AND end_time IS NOT NULL
        ORDER BY end_time DESC
        LIMIT 30
        """,
        (doctor_id,),
    )
    if not tokens:
        return {'fatigue_risk': False, 'reason': None, 'consecutive': 0, 'continuous_min': 0}

    parsed = []
    for t in tokens:
        st = _parse_iso(t['start_time'])
        et = _parse_iso(t['end_time'])
        if st and et:
            parsed.append((st, et))
    if not parsed:
        return {'fatigue_risk': False, 'reason': None, 'consecutive': 0, 'continuous_min': 0}

    parsed.sort(key=lambda x: x[0])
    consecutive = 1
    max_consecutive = 1
    continuous_start = parsed[0][0]
    last_end = parsed[0][1]
    for i in range(1, len(parsed)):
        st, et = parsed[i]
        gap_min = _minutes(st - last_end)
        if gap_min is not None and gap_min < FATIGUE_BREAK_MIN:
            consecutive += 1
        else:
            max_consecutive = max(max_consecutive, consecutive)
            consecutive = 1
            continuous_start = st
        last_end = et
    max_consecutive = max(max_consecutive, consecutive)
    continuous_min = _minutes(last_end - continuous_start) or 0

    fatigue = False
    reason = None
    if max_consecutive > FATIGUE_MAX_CONSECUTIVE_PATIENTS:
        fatigue = True
        reason = f"Consecutive patients: {max_consecutive}"
    elif continuous_min > FATIGUE_MAX_CONTINUOUS_MIN:
        fatigue = True
        reason = f"Continuous service: {int(continuous_min)} min"

    return {'fatigue_risk': fatigue, 'reason': reason, 'consecutive': max_consecutive, 'continuous_min': int(round(continuous_min))}


def simulate_queue_metrics(*, load_increase_pct: float, reduce_doctors: int, emergency_pct: float, followup_return_pct: float):
    rows = query_db('SELECT * FROM tokens ORDER BY generated_time DESC')
    active_doctors = get_active_doctors()
    active_count = max(1, len(active_doctors) - int(reduce_doctors or 0))

    waiting = [r for r in rows if r['status'] in ('Waiting', 'Generated')]
    base_queue = len(waiting)
    base_avg_service = calculate_average_service_time(None) or 1.0
    base_est_wait = round((base_queue * base_avg_service) / active_count, 2)

    extra_from_load = int(round(base_queue * (load_increase_pct / 100.0)))
    extra_from_followup = int(round(base_queue * (followup_return_pct / 100.0)))
    emergency_share = _clamp(emergency_pct / 100.0, 0.0, 1.0)

    sim_queue = base_queue + extra_from_load + extra_from_followup
    sim_est_wait = round((sim_queue * base_avg_service) / active_count, 2)

    sla_min = SLA_WAIT_THRESHOLD_MIN
    load_index = round(sim_est_wait / float(sla_min), 2) if sla_min else None
    breach_prob = _clamp((sim_est_wait - sla_min) / float(sla_min) if sla_min else 0.0, 0.0, 2.0)
    breach_prob = round((_clamp(breach_prob, 0.0, 2.0) / 2.0) * 100.0, 1)

    required_doctors = int(max(1, math.ceil((sim_queue * base_avg_service) / float(sla_min or 1))))

    risk = 'Low'
    if load_index is not None and load_index >= 1.3:
        risk = 'High'
    elif load_index is not None and load_index >= 1.05:
        risk = 'Moderate'

    recommended_add = max(0, required_doctors - active_count)
    action = 'No action needed'
    if recommended_add > 0:
        action = f"Add {recommended_add} doctors"

    before = {
        'queue_len': base_queue,
        'avg_service_min': round(base_avg_service, 2),
        'active_doctors': len(active_doctors),
        'estimated_wait_min': base_est_wait,
    }
    after = {
        'queue_len': sim_queue,
        'active_doctors': active_count,
        'estimated_wait_min': sim_est_wait,
        'emergency_share_pct': round(emergency_share * 100.0, 1),
        'load_index': load_index,
        'sla_breach_probability_pct': breach_prob,
        'required_doctors_for_sla': required_doctors,
    }
    return {
        'before': before,
        'after': after,
        'risk': risk,
        'recommended_action': action,
    }


def doctor_overload_status(doctor_id: int):
    qlen = get_doctor_queue_length(doctor_id)
    avg_wait = calculate_doctor_avg_waiting_time(doctor_id)
    # Overload if queue length > threshold OR avg wait > SLA
    overload = (qlen > CONGESTION_QUEUE_LEN_THRESHOLD) or (avg_wait is not None and avg_wait > WAITING_SLA)
    return {
        'overload': overload,
        'queue_len': qlen,
        'avg_wait_min': round(avg_wait, 2) if avg_wait is not None else None,
        'sla_min': WAITING_SLA,
    }


def smart_reassignment_suggestion(overloaded_doctor_id: int):
    """Find best candidate in same department for reassignment (display-only)."""
    # Get department of overloaded doctor
    doctor_row = query_db("SELECT department_id FROM doctors WHERE id=?", (overloaded_doctor_id,), one=True)
    if not doctor_row or not doctor_row['department_id']:
        return None
    dept_id = doctor_row['department_id']
    candidates = get_doctors_by_department(dept_id)
    # Exclude the overloaded doctor
    candidates = [d for d in candidates if int(d['id']) != int(overloaded_doctor_id)]
    if not candidates:
        return None
    # Rank by queue length (lowest first)
    ranked = sorted(candidates, key=lambda d: get_doctor_queue_length(int(d['id'])))
    best = ranked[0]
    # Find next normal-priority token for overloaded doctor
    next_token = query_db(
        "SELECT id, token_number FROM tokens WHERE doctor_id=? AND priority='normal' AND status IN ('Waiting','Generated') ORDER BY generated_time ASC LIMIT 1",
        (overloaded_doctor_id,),
        one=True,
    )
    return {
        'type': 'Reassignment',
        'from_doctor': overloaded_doctor_id,
        'to_doctor': int(best['id']),
        'to_doctor_name': best['name'],
        'affected_token_id': next_token['id'] if next_token else None,
        'affected_token_number': next_token['token_number'] if next_token else None,
    }


def log_audit_event(event_type: str, details: str):
    db = get_db()
    db.execute(
        "INSERT INTO audit_logs (event_type, details, created_at) VALUES (?,?,?)",
        (event_type, details, datetime.now().isoformat()),
    )
    db.commit()


def apply_reassignment(token_id: int, to_doctor_id: int, manager_username: str | None = None):
    """Apply reassignment and log it. Used by manager approval or auto-balance."""
    db = get_db()
    db.execute("UPDATE tokens SET doctor_id=? WHERE id=?", (to_doctor_id, token_id))
    db.commit()
    details = f"Token {token_id} reassigned to doctor {to_doctor_id}"
    if manager_username:
        details = f"Approved by {manager_username}: {details}"
    log_audit_event('REASSIGNMENT', details)
    push_event({'type': 'reassignment', 'token_id': token_id, 'to_doctor_id': to_doctor_id})
    return True


def _parse_iso(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _minutes(delta: timedelta | None):
    if delta is None:
        return None
    return delta.total_seconds() / 60.0


def _log_anomaly(token_row, anomaly_type: str, observed_value: float | None, threshold_value: float | None):
    """Persist anomaly in SQLite. Uses INSERT OR IGNORE to avoid duplicates per token+type."""
    db = get_db()
    db.execute(
        """
        INSERT OR IGNORE INTO anomaly_logs
            (token_id, token_number, service_name, handled_by, anomaly_type, observed_value, threshold_value, created_at)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            token_row['id'] if token_row else None,
            token_row['token_number'] if token_row else None,
            token_row['service_name'] if token_row else None,
            token_row['handled_by'] if token_row else None,
            anomaly_type,
            observed_value,
            threshold_value,
            datetime.now().isoformat(),
        ),
    )
    db.commit()


def _compute_predictive_wait_minutes(service_name: str, generated_time_iso: str):
    """Predictive Waiting Time Engine (heuristic).

    EstimatedWaitTime = AvgServiceTime(service) * tokens_ahead_in_queue

    Production note:
    - In real deployments, this can be replaced by ML regression / queueing models that learn
      non-linear effects (staffing, time-of-day, case mix) and provide confidence intervals.
    """
    gen_dt = _parse_iso(generated_time_iso)
    if not gen_dt:
        return None

    # Backward-compatible: use the requested estimator.
    # (We keep this wrapper because other parts of the app call it.)
    tokens_ahead = None

    return calculate_estimated_wait_time(service_name, generated_time_iso)


def _compute_advanced_analytics(rows):
    """Smart Queue Intelligence + decision support analytics for the Admin dashboard."""
    today = date.today()
    today_tokens = [r for r in rows if _parse_iso(r['generated_time']) and _parse_iso(r['generated_time']).date() == today]
    followup_today = [r for r in today_tokens if ('is_followup' in r.keys()) and int(r['is_followup'] or 0) == 1]

    risk_distribution = {'Emergency': 0, 'Chronic Illness': 0, 'Senior Citizen': 0, 'Regular': 0}
    high_risk_waiting = 0
    for r in today_tokens:
        cat = _safe_patient_category(r['patient_category'] if 'patient_category' in r.keys() else 'Regular')
        risk_distribution[cat] = risk_distribution.get(cat, 0) + 1
        try:
            rs = int(r['risk_score']) if 'risk_score' in r.keys() and r['risk_score'] is not None else int(compute_risk_and_priority(dict(r)).get('risk_score') or 0)
        except Exception:
            rs = 0
        if r['status'] in ('Waiting', 'Generated') and rs >= 80:
            high_risk_waiting += 1

    waiting_mins = []
    service_mins = []
    wait_violations = []
    service_violations = []

    hourly_counts = {h: 0 for h in range(24)}
    active_in_queue = 0

    per_service_service_mins = {}
    per_service_wait_mins = {}

    last_activity_dt = None

    for r in today_tokens:
        gen = _parse_iso(r['generated_time'])
        start = _parse_iso(r['start_time'])
        end = _parse_iso(r['end_time'])

        if gen:
            hourly_counts[gen.hour] = hourly_counts.get(gen.hour, 0) + 1

        if r['status'] in ('Generated', 'Waiting', 'InProgress'):
            active_in_queue += 1

        # System last activity = any start/end event.
        for dt in (start, end):
            if dt and (last_activity_dt is None or dt > last_activity_dt):
                last_activity_dt = dt

        # Waiting time:
        # - New doctor-based flow: assigned_time -> start_time
        # - Backward-compatible: generated_time -> start_time
        assigned = _parse_iso(r['assigned_time']) if 'assigned_time' in r.keys() else None
        ref = assigned or gen
        if ref and start and start >= ref:
            w = _minutes(start - ref)
            waiting_mins.append(w)
            per_service_wait_mins.setdefault(r['service_name'], []).append(w)
            if w > SLA_WAIT_THRESHOLD_MIN:
                wait_violations.append({'id': r['id'], 'token': r['token_number'], 'wait_min': round(w, 2), 'service': r['service_name']})
                _log_anomaly(r, 'SLA_WAIT_BREACH', round(w, 2), SLA_WAIT_THRESHOLD_MIN)

        if start and end and end >= start:
            smin = _minutes(end - start)
            service_mins.append(smin)
            per_service_service_mins.setdefault(r['service_name'], []).append(smin)
            if smin > SLA_SERVICE_THRESHOLD_MIN:
                service_violations.append({'id': r['id'], 'token': r['token_number'], 'service_min': round(smin, 2), 'service': r['service_name']})
                _log_anomaly(r, 'SLA_SERVICE_BREACH', round(smin, 2), SLA_SERVICE_THRESHOLD_MIN)

    avg_wait = round(sum(waiting_mins) / len(waiting_mins), 2) if waiting_mins else None
    avg_service = round(sum(service_mins) / len(service_mins), 2) if service_mins else None

    # Estimated average waiting time for the queue (simple heuristic).
    # Production note: can be replaced with ML-based prediction / queueing simulation.
    avg_service_overall = calculate_average_service_time(None)
    waiting_queue_len = sum(1 for r in today_tokens if r['status'] in ('Waiting', 'Generated'))
    estimated_avg_wait_min = round(avg_service_overall * waiting_queue_len, 1) if avg_service_overall is not None else None

    # Peak hour detection: best contiguous 2-hour window by arrivals.
    best_window = None
    best_sum = -1
    for h in range(23):
        s = hourly_counts.get(h, 0) + hourly_counts.get(h + 1, 0)
        if s > best_sum:
            best_sum = s
            best_window = (h, h + 1)

    peak_text = None
    if best_window and best_sum > 0:
        h1, h2 = best_window
        peak_text = f"Peak load detected between {h1:02d}:00 - {h2 + 1:02d}:00."

    # Queue congestion: either avg wait breaches SLA OR queue length too high.
    congestion = False
    congestion_reasons = []
    if avg_wait is not None and avg_wait > SLA_WAIT_THRESHOLD_MIN:
        congestion = True
        congestion_reasons.append('Waiting time exceeds SLA.')
    if active_in_queue >= CONGESTION_QUEUE_LEN_THRESHOLD:
        congestion = True
        congestion_reasons.append('High number of active tokens in queue.')
    congestion_text = None
    if congestion:
        congestion_text = 'Queue congestion alert: ' + ' '.join(congestion_reasons)

    # Bottleneck detection: service with highest avg service duration vs overall avg.
    bottleneck_text = None
    bottleneck_service = None
    bottleneck_pct = None
    if per_service_service_mins:
        svc_avgs = {k: (sum(v) / len(v)) for k, v in per_service_service_mins.items() if v}
        if svc_avgs:
            bottleneck_service = max(svc_avgs, key=lambda k: svc_avgs[k])
            overall = (sum(service_mins) / len(service_mins)) if service_mins else None
            if overall and overall > 0:
                bottleneck_pct = round(((svc_avgs[bottleneck_service] - overall) / overall) * 100.0, 1)
                if bottleneck_pct > 5:
                    bottleneck_text = f"{bottleneck_service} services have {bottleneck_pct}% higher service duration."

    # Agent idle time detection (heuristic).
    # If nothing is InProgress and last activity is older than threshold, call it idle.
    in_progress = any(r['status'] == 'InProgress' for r in today_tokens)
    idle_text = None
    if not in_progress:
        if last_activity_dt:
            idle_min = _minutes(datetime.now() - last_activity_dt)
            if idle_min is not None and idle_min >= IDLE_THRESHOLD_MIN:
                idle_text = f"Agent idle for {int(idle_min)} minutes."
        else:
            # No activity yet today.
            idle_text = None

    # Staff optimization recommendation.
    staff_reco = None
    sla_status = check_sla_status(avg_wait, avg_service, queue_len=waiting_queue_len)
    if not isinstance(sla_status, dict):
        sla_status = {
            'breach': False,
            'title': 'All services operating within SLA compliance.',
            'recommendation': None,
        }
    if sla_status.get('breach') and peak_text:
        staff_reco = 'System recommends adding 1 additional service counter during peak hours.'
        # Production note: real systems use demand forecasting + staffing optimization (integer programming / simulation).

    # Doctor-wise SLA monitoring + display-only emergency recommendation.
    doctor_stats = []
    any_doctor_breach = False
    dn = _doctor_name_map()
    for d in get_active_doctors():
        did = int(d['id'])
        qlen = get_doctor_queue_length(did)
        avg_w = calculate_doctor_avg_waiting_time(did)
        avg_s = calculate_doctor_avg_service_time(did)
        breach = (avg_w is not None and avg_w > WAITING_SLA)
        any_doctor_breach = any_doctor_breach or breach
        overload = doctor_overload_status(did)['overload']
        reco = emergency_reallocation_recommendation(did) if overload else None
        fatigue = _fatigue_for_doctor(did)
        doctor_stats.append({
            'doctor_id': did,
            'doctor_name': dn.get(did, d['name']),
            'specialization': d['specialization'],
            'queue_len': qlen,
            'avg_wait_min': round(avg_w, 2) if avg_w is not None else None,
            'avg_service_min': round(avg_s, 2) if avg_s is not None else None,
            'sla_breach': breach,
            'sla_min': WAITING_SLA,
            'overload': overload,
            'recommendation': reco,
            'fatigue_risk': bool(fatigue.get('fatigue_risk')),
            'fatigue_reason': fatigue.get('reason'),
            'fatigue_consecutive': fatigue.get('consecutive'),
            'fatigue_continuous_min': fatigue.get('continuous_min'),
        })

        # Log + broadcast fatigue risk (best-effort; avoid spamming by checking last event)
        if fatigue.get('fatigue_risk'):
            try:
                last = query_db(
                    "SELECT created_at FROM audit_logs WHERE event_type='FATIGUE_RISK' AND details LIKE ? ORDER BY created_at DESC LIMIT 1",
                    (f"%doctor_id={did}%",),
                    one=True,
                )
                should_log = True
                if last and last.get('created_at'):
                    last_dt = _parse_iso(last['created_at'])
                    if last_dt and (datetime.now() - last_dt).total_seconds() < 15 * 60:
                        should_log = False
                if should_log:
                    log_audit_event('FATIGUE_RISK', f"doctor_id={did} reason={fatigue.get('reason')}")
                    push_event({'type': 'fatigue_risk', 'doctor_id': did, 'reason': fatigue.get('reason')})
            except Exception:
                pass

    # Department-wise SLA breach detection
    department_stats = []
    any_dept_breach = False
    for dept in get_departments():
        dept_id = int(dept['id'])
        status = department_sla_status(dept_id)
        any_dept_breach = any_dept_breach or status['breach']
        department_stats.append({
            'department_id': dept_id,
            'department_name': dept['name'],
            'sla_min': status['sla_min'],
            'estimated_wait_min': status['estimated_wait_min'],
            'breach': status['breach'],
        })

    # Manager recommendations (smart reassignments for overloaded doctors)
    manager_recommendations = []
    for d in get_active_doctors():
        did = int(d['id'])
        if doctor_overload_status(did)['overload']:
            sug = smart_reassignment_suggestion(did)
            if sug:
                manager_recommendations.append(sug)

    # Auto-balance: apply first recommendation if enabled and no emergency/follow-up tokens are waiting
    if AUTO_BALANCE_ENABLED and manager_recommendations:
        first = manager_recommendations[0]
        # Verify the token is normal priority and not emergency/follow-up
        token_row = query_db("SELECT priority FROM tokens WHERE id=?", (first['affected_token_id'],), one=True)
        if token_row and token_row['priority'] == 'normal':
            apply_reassignment(first['affected_token_id'], first['to_doctor'])
            # Remove applied recommendation from list
            manager_recommendations = [r for r in manager_recommendations if r['affected_token_id'] != first['affected_token_id']]

    # Service efficiency ranking (fastest to slowest).
    service_ranking = []
    if per_service_service_mins:
        for svc, mins in per_service_service_mins.items():
            if mins:
                service_ranking.append({'service_name': svc, 'avg_service_min': round(sum(mins) / len(mins), 2), 'completed': len(mins)})
        service_ranking.sort(key=lambda x: x['avg_service_min'])

    # Agent performance ranking: tokens handled per hour (today).
    agent_perf = {}
    for r in today_tokens:
        if r['status'] != 'Completed':
            continue
        # Hackathon/demo mode: older rows may not have `handled_by` populated.
        # We still include them under an 'Unknown' bucket so the ranking UI is not empty.
        agent_name = r['handled_by'] if r['handled_by'] else 'Unknown'
        start = _parse_iso(r['start_time'])
        end = _parse_iso(r['end_time'])
        if not start or not end:
            continue
        ap = agent_perf.setdefault(agent_name, {'count': 0, 'first': None, 'last': None})
        ap['count'] += 1
        ap['first'] = start if ap['first'] is None or start < ap['first'] else ap['first']
        ap['last'] = end if ap['last'] is None or end > ap['last'] else ap['last']

    agent_ranking = []
    for agent, d in agent_perf.items():
        span_hours = None
        if d['first'] and d['last'] and d['last'] > d['first']:
            span_hours = (d['last'] - d['first']).total_seconds() / 3600.0
        # Avoid division by tiny spans; fall back to count.
        tph = (d['count'] / span_hours) if (span_hours and span_hours >= 0.25) else float(d['count'])
        agent_ranking.append({'agent': agent, 'tokens_per_hour': round(tph, 2), 'tokens_completed': d['count']})
    agent_ranking.sort(key=lambda x: x['tokens_per_hour'], reverse=True)

    # Advanced Agent Performance Analytics
    # Agent is identified by tokens.handled_by (service username).
    agent_stats = {}
    for r in today_tokens:
        if r['status'] != 'Completed':
            continue
        agent_name = r['handled_by'] if r['handled_by'] else 'Unknown'
        st = _parse_iso(r['start_time'])
        et = _parse_iso(r['end_time'])
        if not st or not et or et < st:
            continue

        sdur = None
        try:
            if 'service_duration' in r.keys() and r['service_duration'] is not None:
                sdur = float(r['service_duration'])
            else:
                sdur = float(_minutes(et - st))
        except Exception:
            sdur = None

        cat = _safe_patient_category(r['patient_category'] if 'patient_category' in r.keys() else 'Regular')
        is_emergency = (cat == 'Emergency')
        is_followup = int(r['is_followup'] or 0) == 1 if 'is_followup' in r.keys() else False

        try:
            rs = int(r['risk_score']) if 'risk_score' in r.keys() and r['risk_score'] is not None else int(compute_risk_and_priority(dict(r)).get('risk_score') or 0)
        except Exception:
            rs = 0

        try:
            sb = int(r['sla_breached']) if 'sla_breached' in r.keys() and r['sla_breached'] is not None else int(_compute_sla_breached_for_token_row(dict(r)))
        except Exception:
            sb = 0

        a = agent_stats.setdefault(
            agent_name,
            {
                'served': 0,
                'service_mins': [],
                'sla_breaches': 0,
                'non_emergency_served': 0,
                'followups': 0,
                'risk_sum': 0,
                'risk_count': 0,
                'high_risk_served': 0,
                'high_risk_sla_breaches': 0,
                'emergency_served': 0,
            },
        )

        a['served'] += 1
        if sdur is not None:
            a['service_mins'].append(float(sdur))

        if is_followup:
            a['followups'] += 1

        a['risk_sum'] += int(rs)
        a['risk_count'] += 1

        if rs >= 80:
            a['high_risk_served'] += 1
            if sb:
                a['high_risk_sla_breaches'] += 1

        if is_emergency:
            a['emergency_served'] += 1
        else:
            a['non_emergency_served'] += 1
            if sb:
                a['sla_breaches'] += 1

    total_served = sum(v['served'] for v in agent_stats.values())
    agent_count = len(agent_stats) if agent_stats else 0
    ideal_share = (total_served / agent_count) if agent_count else 0
    overall_avg_service = avg_service if avg_service is not None else None

    def _clamp01(x):
        try:
            return max(0.0, min(1.0, float(x)))
        except Exception:
            return 0.0

    agent_performance = []
    at_risk_agents = []
    for agent, d in agent_stats.items():
        served = int(d['served'])
        avg_service_min_agent = (sum(d['service_mins']) / len(d['service_mins'])) if d['service_mins'] else None
        non_em = int(d['non_emergency_served'])
        breaches = int(d['sla_breaches'])
        sla_compliance = (1.0 - (breaches / non_em)) if non_em > 0 else 1.0

        followup_rate = (int(d['followups']) / served) if served > 0 else 0.0
        avg_risk = (int(d['risk_sum']) / int(d['risk_count'])) if int(d['risk_count']) > 0 else 0.0

        # Component scores (0-100)
        sla_score = round(_clamp01(sla_compliance) * 100.0, 2)

        service_eff = 50.0
        if avg_service_min_agent is not None and overall_avg_service is not None and overall_avg_service > 0:
            # Better than overall => up to 100, worse => down toward 0
            ratio = avg_service_min_agent / overall_avg_service
            service_eff = max(0.0, min(100.0, 100.0 - ((ratio - 1.0) * 60.0)))

        balanced_load = 100.0
        if ideal_share and ideal_share > 0:
            balanced_load = max(0.0, min(100.0, 100.0 - (abs(served - ideal_share) / ideal_share) * 100.0))

        # High-risk handling score: reward taking high-risk, but don't penalize if they have high risk.
        # Use share of high-risk handled scaled by max among agents.
        max_high = max((x['high_risk_served'] for x in agent_stats.values()), default=0)
        high_risk_score = (int(d['high_risk_served']) / max_high * 100.0) if max_high > 0 else 0.0

        # Cancellation not modeled in this app; keep neutral.
        low_cancel_score = 100.0

        # Fatigue stability: flag if any doctor currently in fatigue risk that this agent has served today.
        fatigue_flag = 0
        try:
            fatigued_doctors = {int(x['doctor_id']) for x in doctor_stats if x.get('fatigue_risk')}
            if fatigued_doctors:
                for r in today_tokens:
                    if r['status'] == 'Completed' and (r['handled_by'] if r['handled_by'] else 'Unknown') == agent:
                        if r.get('doctor_id') in fatigued_doctors:
                            fatigue_flag = 1
                            break
        except Exception:
            fatigue_flag = 0
        fatigue_stability = 50.0 if fatigue_flag else 100.0

        efficiency = (
            0.30 * sla_score +
            0.25 * service_eff +
            0.15 * balanced_load +
            0.15 * high_risk_score +
            0.10 * low_cancel_score +
            0.05 * fatigue_stability
        )
        efficiency = max(0.0, min(100.0, round(efficiency, 2)))

        if efficiency >= 90:
            tier = 'Elite'
        elif efficiency >= 75:
            tier = 'Strong'
        elif efficiency >= 60:
            tier = 'Moderate'
        else:
            tier = 'Needs Improvement'

        insights = []
        if ideal_share and served > ideal_share * 1.5:
            insights.append('Overloaded: suggest rotation / add support.')
        if ideal_share and served < ideal_share * 0.5:
            insights.append('Underutilized: suggest load transfer.')
        if sla_score < 80:
            insights.append('SLA compliance dropping: suggest process review.')

        row = {
            'agent': agent,
            'served': served,
            'avg_service_min': round(avg_service_min_agent, 2) if avg_service_min_agent is not None else None,
            'sla_compliance_pct': round(sla_score, 2),
            'followup_rate_pct': round(followup_rate * 100.0, 2),
            'avg_risk_handled': round(avg_risk, 2),
            'efficiency_score': efficiency,
            'tier': tier,
            'fatigue_warning': bool(fatigue_flag),
            'insights': insights,
        }
        agent_performance.append(row)
        if efficiency < 60 or sla_score < 70:
            at_risk_agents.append(row)

    agent_performance.sort(key=lambda x: x['efficiency_score'], reverse=True)
    top_agents = agent_performance[:3]

    # Operational health score (0-100).
    penalties = 0.0
    if avg_wait is not None:
        penalties += min(40.0, (avg_wait / SLA_WAIT_THRESHOLD_MIN) * 40.0)
    if avg_service is not None:
        penalties += min(30.0, (avg_service / SLA_SERVICE_THRESHOLD_MIN) * 30.0)
    if congestion:
        penalties += 20.0
    violations = len(wait_violations) + len(service_violations)
    penalties += min(10.0, violations * 2.0)
    op_score = max(0, min(100, int(round(100.0 - penalties))))

    # Smart Insight Generator (simple stats; no ML).
    insights = []
    if peak_text:
        insights.append(peak_text)
    if congestion_text:
        insights.append(congestion_text)
    if idle_text:
        insights.append(idle_text)
    if bottleneck_text:
        insights.append(bottleneck_text)

    # Morning vs rest of day traffic comparison.
    morning = sum(hourly_counts[h] for h in range(6, 12))
    afternoon = sum(hourly_counts[h] for h in range(12, 18))
    if afternoon > 0:
        diff = ((morning - afternoon) / afternoon) * 100.0
        if diff >= 30:
            insights.append(f"Morning hours show {int(round(diff))}% higher patient traffic.")

    # Today vs yesterday service efficiency change.
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    y_rows = query_db(
        """
        SELECT start_time, end_time
        FROM tokens
        WHERE status='Completed'
          AND date(generated_time) = ?
          AND start_time IS NOT NULL
          AND end_time IS NOT NULL
        """,
        (yesterday,),
    )
    y_svc = []
    for r in y_rows:
        s = _parse_iso(r['start_time'])
        e = _parse_iso(r['end_time'])
        if s and e and e >= s:
            y_svc.append(_minutes(e - s))
    if avg_service is not None and y_svc:
        y_avg = sum(y_svc) / len(y_svc)
        if y_avg and y_avg > 0:
            change = ((y_avg - avg_service) / y_avg) * 100.0
            if change >= 5:
                insights.append(f"Service efficiency improved by {int(round(change))}% compared to yesterday.")
            elif change <= -5:
                insights.append(f"Service efficiency dropped by {int(round(abs(change)))}% compared to yesterday.")

    # Queue Health Score + trend
    doctor_queue_lens = [int(x.get('queue_len') or 0) for x in doctor_stats]
    overload_flags = [bool(x.get('overload')) for x in doctor_stats]
    health_score = compute_queue_health_score(
        sla_breach=bool(sla_status.get('breach')) if isinstance(sla_status, dict) else False,
        total_today=len(today_tokens),
        avg_wait=avg_wait,
        sla_wait_min=SLA_WAIT_THRESHOLD_MIN,
        doctor_queue_lens=doctor_queue_lens,
        overload_flags=overload_flags,
    )
    try:
        _upsert_today_health_score(health_score)
    except Exception:
        pass
    health_trend = _get_health_trend(7)

    return {
        'total_today': len(today_tokens),
        'avg_wait': avg_wait,
        'avg_service': avg_service,
        'wait_violations': wait_violations,
        'service_violations': service_violations,
        'hourly_counts': [{'hour': h, 'count': hourly_counts.get(h, 0)} for h in range(24)],
        'estimated_avg_wait_min': estimated_avg_wait_min,
        'peak_text': peak_text,
        'congestion_text': congestion_text,
        'idle_text': idle_text,
        'bottleneck_text': bottleneck_text,
        'op_score': op_score,
        'insights': insights,
        'staff_reco': staff_reco,
        'sla_wait_threshold': SLA_WAIT_THRESHOLD_MIN,
        'sla_service_threshold': SLA_SERVICE_THRESHOLD_MIN,
        'sla_status': sla_status,
        'doctor_stats': doctor_stats,
        'any_doctor_sla_breach': any_doctor_breach,
        'department_stats': department_stats,
        'any_department_sla_breach': any_dept_breach,
        'manager_recommendations': manager_recommendations,
        'auto_balance_enabled': AUTO_BALANCE_ENABLED,
        'followup_count_today': len(followup_today),
        'queue_health_score': health_score,
        'queue_health_trend': health_trend,
        'risk_distribution': risk_distribution,
        'high_risk_waiting': high_risk_waiting,
        'agent_ranking': agent_ranking,
        'agent_performance': agent_performance,
        'top_agents': top_agents,
        'at_risk_agents': at_risk_agents,
    }
def login_required(role=None):
    def decorator(f):
        def wrapped(*args, **kwargs):
            wants_json = (
                request.path.startswith('/api/')
                or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                or request.accept_mimetypes.best == 'application/json'
            )
            if 'user_id' not in session:
                if wants_json:
                    return app.response_class(json.dumps({'error': 'auth_required'}), status=401, mimetype='application/json')
                return redirect(url_for('login'))
            if role and session.get('role') != role:
                if wants_json:
                    return app.response_class(json.dumps({'error': 'forbidden'}), status=403, mimetype='application/json')
                # If role doesn't match, redirect to their home
                r = session.get('role')
                if r == 'Admin':
                    return redirect(url_for('dashboard'))
                if r == 'Receptionist':
                    return redirect(url_for('generate'))
                if r == 'Service':
                    return redirect(url_for('service_panel'))
                if r == 'Manager':
                    return redirect(url_for('manager'))
                if r == 'Patient':
                    return redirect(url_for('patient_panel'))
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        wrapped.__name__ = f.__name__
        return wrapped
    return decorator


def role_required(required_role):
    def decorator(f):
        def wrapped(*args, **kwargs):
            wants_json = (
                request.path.startswith('/api/')
                or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                or request.accept_mimetypes.best == 'application/json'
            )
            if 'user_id' not in session:
                if wants_json:
                    return app.response_class(json.dumps({'error': 'auth_required'}), status=401, mimetype='application/json')
                return redirect(url_for('login'))
            if session.get('role') != required_role:
                if wants_json:
                    return app.response_class(json.dumps({'error': 'forbidden'}), status=403, mimetype='application/json')
                # Redirect to their home if role mismatch
                r = session.get('role')
                if r == 'Admin':
                    return redirect(url_for('dashboard'))
                if r == 'Receptionist':
                    return redirect(url_for('generate'))
                if r == 'Service':
                    return redirect(url_for('service_panel'))
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        wrapped.__name__ = f.__name__
        return wrapped
    return decorator


@app.route('/simulation')
@login_required(role='Manager')
def simulation_lab():
    return render_template('simulation.html')


@app.route('/api/simulate', methods=['POST'])
@login_required(role='Manager')
@limiter.limit('30/minute')
def api_simulate():
    data = request.get_json(force=True) or {}
    _log_suspicious_client_fields(data, {'load_increase_pct', 'reduce_doctors', 'emergency_pct', 'followup_return_pct'}, 'api_simulate')
    load_increase_pct = float(data.get('load_increase_pct') or 0)
    reduce_doctors = int(data.get('reduce_doctors') or 0)
    emergency_pct = float(data.get('emergency_pct') or 0)
    followup_return_pct = float(data.get('followup_return_pct') or 0)
    result = simulate_queue_metrics(
        load_increase_pct=load_increase_pct,
        reduce_doctors=reduce_doctors,
        emergency_pct=emergency_pct,
        followup_return_pct=followup_return_pct,
    )
    return app.response_class(json.dumps(result), mimetype='application/json')


@app.route('/api/fatigue')
@login_required()
def api_fatigue():
    # Allow Service/Admin/Manager (Receptionist can also see; harmless)
    out = []
    for d in get_active_doctors():
        did = int(d['id'])
        f = _fatigue_for_doctor(did)
        out.append({
            'doctor_id': did,
            'doctor_name': d['name'],
            'fatigue_risk': bool(f.get('fatigue_risk')),
            'reason': f.get('reason'),
            'consecutive': f.get('consecutive'),
            'continuous_min': f.get('continuous_min'),
        })
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/api/tokens_by_doctor')
@login_required()
def api_tokens_by_doctor():
    rows = query_db(
        """
        SELECT t.*, d.name AS doctor_name
        FROM tokens t
        LEFT JOIN doctors d ON d.id = t.doctor_id
        ORDER BY
          COALESCE(t.doctor_id, 999999) ASC,
          CASE
            WHEN t.status='InProgress' THEN 0
            WHEN t.status IN ('Waiting','Generated') THEN 1
            ELSE 2
          END,
          t.priority_level ASC,
          t.ai_priority_level ASC,
          t.generated_time ASC
        """
    )

    grouped = {}
    for r in rows:
        calc = compute_risk_and_priority(dict(r))
        did = int(r['doctor_id']) if r['doctor_id'] is not None else 0
        dname = r['doctor_name'] if 'doctor_name' in r.keys() and r['doctor_name'] else ('Unassigned' if did == 0 else 'Unknown')
        gk = str(did)
        bucket = grouped.setdefault(
            gk,
            {
                'doctor_id': did,
                'doctor_name': dname,
                'tokens': [],
            },
        )
        bucket['tokens'].append(
            {
                'id': int(r['id']),
                'token_number': r['token_number'],
                'service_name': r['service_name'],
                'generated_time': r['generated_time'],
                'start_time': r['start_time'],
                'end_time': r['end_time'],
                'status': r['status'],
                'doctor_id': did if did else None,
                'doctor_name': dname,
                'patient_name': r['patient_name'] if 'patient_name' in r.keys() else None,
                'is_followup': int(r['is_followup']) if 'is_followup' in r.keys() else 0,
                'followup_of_token_id': r['followup_of_token_id'] if 'followup_of_token_id' in r.keys() else None,
                'priority_level': int(r['priority_level']) if 'priority_level' in r.keys() else 3,
                'patient_category': r['patient_category'] if 'patient_category' in r.keys() else 'Regular',
                'risk_score': int(r['risk_score']) if 'risk_score' in r.keys() and r['risk_score'] is not None else int(calc['risk_score']),
                'ai_priority_level': int(r['ai_priority_level']) if 'ai_priority_level' in r.keys() and r['ai_priority_level'] is not None else int(calc['ai_priority_level']),
                'ai_priority_reason': calc.get('reason'),
            }
        )

    ordered = list(grouped.values())
    ordered.sort(key=lambda x: (0 if int(x['doctor_id'] or 0) > 0 else 1, int(x['doctor_id'] or 0), x['doctor_name']))
    return app.response_class(json.dumps(ordered), mimetype='application/json')


@app.route('/')
def index():
    if 'user_id' in session:
        role = session.get('role')
        if role == 'Admin':
            return redirect(url_for('dashboard'))
        if role == 'Manager':
            return redirect(url_for('manager'))
        if role == 'Receptionist':
            return redirect(url_for('generate'))
        if role == 'Service':
            return redirect(url_for('service_panel'))
        if role == 'Patient':
            return redirect(url_for('patient_panel'))
    return redirect(url_for('login'))


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        register_as = (request.form.get('register_as') or 'Staff').strip()
        role = (request.form.get('role') or '').strip()
        full_name = (request.form.get('full_name') or '').strip() or None
        phone_number = (request.form.get('phone_number') or '').strip() or None

        if register_as == 'Patient':
            role = 'Patient'
            if not full_name or not phone_number:
                return render_template('register.html', error='Full Name and Phone are required for Patient')

        if not username or not password or not role:
            return render_template('register.html', error='All fields are required')
        # Password constraints
        if len(password) < 8:
            return render_template('register.html', error='Password must be at least 8 characters')
        if not any(c.islower() for c in password) or not any(c.isupper() for c in password):
            return render_template('register.html', error='Password must include both lower and upper case letters')
        if not any(c.isdigit() for c in password):
            return render_template('register.html', error='Password must include at least one digit')
        existing = query_db('SELECT * FROM users WHERE username=?', (username,), one=True)
        if existing:
            return render_template('register.html', error='Username already exists')
        password_hash = generate_password_hash(password)
        db = get_db()
        db.execute('INSERT INTO users (username, password, role) VALUES (?,?,?)', (username, password_hash, role))
        db.commit()

        if role == 'Patient':
            user_row = query_db('SELECT id FROM users WHERE username=?', (username,), one=True)
            if user_row:
                pid = str(uuid.uuid4())
                now_iso = datetime.now().isoformat()
                try:
                    db.execute(
                        'INSERT OR IGNORE INTO patients (id, user_id, full_name, phone_number, created_at) VALUES (?,?,?,?,?)',
                        (pid, int(user_row['id']), full_name, phone_number, now_iso),
                    )
                    db.commit()
                except Exception:
                    pass
        return redirect(url_for('login'))
    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password']
        user = query_db('SELECT * FROM users WHERE username=?', (username,), one=True)
        if user:
            stored_pw = user['password']
            # If the stored password looks like a werkzeug hash (starts with 'pbkdf2:' or 'argon2'),
            # verify with check_password_hash. Otherwise it may be plaintext from an older DB;
            # in that case compare directly and replace with a hashed password for future safety.
            # Recognize common hash prefixes (pbkdf2, argon2, bcrypt, scrypt)
            is_hashed = (
                stored_pw.startswith('pbkdf2:') or
                stored_pw.startswith('argon2:') or
                stored_pw.startswith('$2b$') or
                stored_pw.startswith('scrypt:')
            )
            valid = False
            if is_hashed:
                valid = check_password_hash(stored_pw, password)
            else:
                # plaintext fallback (upgrade to hashed)
                if stored_pw == password:
                    valid = True
                    # re-hash and store securely
                    new_hash = generate_password_hash(password)
                    db = get_db()
                    db.execute('UPDATE users SET password=? WHERE id=?', (new_hash, user['id']))
                    db.commit()
            if valid:
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['role'] = user['role']
                # role-based redirect
                if user['role'] == 'Admin':
                    return redirect(url_for('dashboard'))
                if user['role'] == 'Manager':
                    return redirect(url_for('manager'))
                if user['role'] == 'Receptionist':
                    return redirect(url_for('generate'))
                if user['role'] == 'Service':
                    return redirect(url_for('service_panel'))
                if user['role'] == 'Patient':
                    return redirect(url_for('patient_panel'))
        return render_template('login.html', error='Invalid credentials')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/manager')
@login_required(role='Manager')
def manager():
    rows = query_db('SELECT * FROM tokens ORDER BY generated_time DESC')
    analytics = _compute_advanced_analytics(rows)
    return render_template(
        'manager.html',
        analytics=analytics,
    )


@app.route('/api/manager')
@login_required(role='Manager')
def api_manager():
    rows = query_db('SELECT * FROM tokens ORDER BY generated_time DESC')
    analytics = _compute_advanced_analytics(rows)
    return app.response_class(json.dumps(analytics), mimetype='application/json')


@app.route('/api/manager/approve', methods=['POST'])
@login_required(role='Manager')
def api_manager_approve():
    data = request.get_json(force=True) or {}
    token_id = int(data.get('token_id') or 0)
    to_doctor_id = int(data.get('to_doctor_id') or 0)
    if not token_id or not to_doctor_id:
        return app.response_class(json.dumps({'status': 'error', 'message': 'Invalid request'}), mimetype='application/json')
    manager_username = session.get('username')
    ok = apply_reassignment(token_id, to_doctor_id, manager_username)
    return app.response_class(json.dumps({'status': 'ok' if ok else 'error'}), mimetype='application/json')


@app.route('/api/manager/ignore', methods=['POST'])
@login_required(role='Manager')
def api_manager_ignore():
    data = request.get_json(force=True) or {}
    token_id = int(data.get('token_id') or 0)
    manager_username = session.get('username')
    if token_id:
        log_audit_event('IGNORE_RECOMMENDATION', f"Ignored by {manager_username}: token {token_id}")
    return app.response_class(json.dumps({'status': 'ok'}), mimetype='application/json')


@app.route('/patient')
@login_required(role='Patient')
def patient_panel():
    depts = query_db('SELECT name FROM departments ORDER BY name ASC')
    departments = [r['name'] for r in depts]
    return render_template('patient_panel.html', departments=departments)


def _pick_doctor_for_department(service_name: str, preferred_doctor_id: int | None):
    dept = query_db('SELECT id, name FROM departments WHERE name=?', (service_name,), one=True)
    if not dept:
        return None
    dept_id = int(dept['id'])

    if preferred_doctor_id:
        row = query_db(
            'SELECT id FROM doctors WHERE id=? AND is_active=1 AND (specialization=? OR department_id=?)',
            (preferred_doctor_id, service_name, dept_id),
            one=True,
        )
        if row:
            return int(row['id'])

    # Default: choose active doctor with smallest queue for this department
    docs = get_active_doctors(service_name)
    if not docs:
        return None
    best_id = None
    best_len = None
    for d in docs:
        did = int(d['id'])
        qlen = get_doctor_queue_length(did)
        if best_len is None or qlen < best_len:
            best_len = qlen
            best_id = did
    return best_id


@app.route('/api/patient/doctors')
@login_required(role='Patient')
def api_patient_doctors():
    service = (request.args.get('service') or '').strip()
    if not service:
        return app.response_class(json.dumps([]), mimetype='application/json')
    rows = get_active_doctors(service)
    out = [{'id': int(r['id']), 'name': r['name']} for r in rows]
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/patient/token', methods=['POST'])
@login_required(role='Patient')
@limiter.limit('10/day')
def patient_token_create():
    user_id = int(session.get('user_id') or 0)
    service = (request.form.get('service') or '').strip()
    preferred_doctor_id = int(request.form.get('preferred_doctor_id') or 0) or None
    patient_name = (request.form.get('patient_name') or '').strip() or None

    if not service:
        return app.response_class(json.dumps({'error': 'service_required'}), status=400, mimetype='application/json')

    dept = query_db('SELECT id FROM departments WHERE name=?', (service,), one=True)
    if not dept:
        return app.response_class(json.dumps({'error': 'invalid_service'}), status=400, mimetype='application/json')

    # Enforce REGULAR only; ignore any client tampering attempts.
    patient_category = 'Regular'

    # Prevent duplicate active token per patient per department.
    dup = query_db(
        """
        SELECT id FROM tokens
        WHERE created_by_role='PATIENT'
          AND created_by_user_id=?
          AND service_name=?
          AND status IN ('Waiting','Generated','InProgress')
        LIMIT 1
        """,
        (user_id, service),
        one=True,
    )
    if dup:
        return app.response_class(json.dumps({'error': 'duplicate_active_token'}), status=409, mimetype='application/json')

    # Max 2 token generations per day per patient.
    today = date.today().isoformat()
    cnt_row = query_db(
        """
        SELECT COUNT(*) AS cnt
        FROM tokens
        WHERE created_by_role='PATIENT'
          AND created_by_user_id=?
          AND date(generated_time)=?
        """,
        (user_id, today),
        one=True,
    )
    try:
        if int(cnt_row['cnt'] or 0) >= 2:
            return app.response_class(json.dumps({'error': 'daily_limit_reached'}), status=429, mimetype='application/json')
    except Exception:
        pass

    selected_doctor_id = _pick_doctor_for_department(service, preferred_doctor_id)
    if not selected_doctor_id:
        if preferred_doctor_id:
            return app.response_class(json.dumps({'error': 'preferred_doctor_unavailable'}), status=400, mimetype='application/json')
        return app.response_class(json.dumps({'error': 'no_doctor_available'}), status=400, mimetype='application/json')

    # Generate token number compatible with existing scheme.
    cur = get_db().execute(
        "SELECT COUNT(*) AS cnt FROM tokens WHERE doctor_id=? AND service_name=?",
        (selected_doctor_id, service),
    )
    row = cur.fetchone()
    count = row['cnt'] if row else 0
    doctor_row = query_db("SELECT name FROM doctors WHERE id=?", (selected_doctor_id,), one=True)
    doctor_name = doctor_row['name'] if doctor_row else 'UNKNOWN'
    doctor_code = doctor_name.split()[-1].upper() if doctor_name else 'UNK'
    service_code = service[:3].upper()
    token_number = f"{service_code}{count+1}-{doctor_code}"

    generated_time = datetime.now().isoformat()
    assigned_time = generated_time
    base_priority_level = 3
    calc = compute_risk_and_priority(
        {
            'patient_category': patient_category,
            'is_followup': 0,
            'doctor_id': selected_doctor_id,
            'service_name': service,
            'generated_time': generated_time,
            'assigned_time': assigned_time,
        }
    )

    db = get_db()
    db.execute(
        'INSERT INTO tokens (token_number, service_name, generated_time, assigned_time, doctor_id, created_at, status, is_followup, followup_of_token_id, priority_level, patient_category, risk_score, ai_priority_level, patient_name, created_by_role, created_by_user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (token_number, service, generated_time, assigned_time, selected_doctor_id, generated_time, 'Waiting', 0, None, base_priority_level, patient_category, int(calc['risk_score']), int(calc['ai_priority_level']), patient_name, 'PATIENT', user_id),
    )
    db.commit()
    push_event({'type': 'token_generated', 'token': token_number, 'service': service, 'created_by_role': 'PATIENT'})
    return app.response_class(json.dumps({'status': 'ok', 'token_number': token_number}), mimetype='application/json')


@app.route('/api/patient/tokens')
@login_required(role='Patient')
def api_patient_tokens():
    user_id = int(session.get('user_id') or 0)
    rows = query_db(
        """
        SELECT t.*, d.name AS doctor_name
        FROM tokens t
        LEFT JOIN doctors d ON d.id=t.doctor_id
        WHERE t.created_by_role='PATIENT'
          AND t.created_by_user_id=?
        ORDER BY t.generated_time DESC
        LIMIT 10
        """,
        (user_id,),
    )
    out = []
    for r in rows:
        out.append(
            {
                'id': int(r['id']),
                'token_number': r['token_number'],
                'service_name': r['service_name'],
                'doctor_id': int(r['doctor_id']) if r['doctor_id'] is not None else None,
                'doctor_name': r['doctor_name'] if 'doctor_name' in r.keys() else None,
                'status': r['status'],
                'generated_time': r['generated_time'],
            }
        )
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/api/patient/token_status')
@login_required(role='Patient')
def api_patient_token_status():
    user_id = int(session.get('user_id') or 0)
    token_id = int(request.args.get('token_id') or 0)
    if not token_id:
        return app.response_class(json.dumps({'error': 'token_id_required'}), status=400, mimetype='application/json')
    row = query_db(
        """
        SELECT t.*, d.name AS doctor_name
        FROM tokens t
        LEFT JOIN doctors d ON d.id=t.doctor_id
        WHERE t.id=?
          AND t.created_by_role='PATIENT'
          AND t.created_by_user_id=?
        LIMIT 1
        """,
        (token_id, user_id),
        one=True,
    )
    if not row:
        return app.response_class(json.dumps({'error': 'not_found'}), status=404, mimetype='application/json')

    did = int(row['doctor_id']) if row['doctor_id'] is not None else None
    now_serving, q = _doctor_queue_with_eta(did, limit=25) if did else (None, [])
    pos = None
    if row['status'] in ('Waiting', 'Generated'):
        i = 1
        for t in q:
            if int(t.get('token_id') or 0) == int(row['id']):
                pos = i
                break
            i += 1

    eta = None
    if pos is not None:
        for t in q:
            if int(t.get('token_id') or 0) == int(row['id']):
                eta = t.get('estimated_start_time')
                break

    out = {
        'id': int(row['id']),
        'token_number': row['token_number'],
        'service_name': row['service_name'],
        'doctor_name': row['doctor_name'] if 'doctor_name' in row.keys() else None,
        'status': row['status'],
        'queue_position': pos,
        'estimated_start_time': eta,
        'now_serving': now_serving,
        'server_time': datetime.now().isoformat(),
    }
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/patient/token/cancel/<int:token_id>', methods=['POST'])
@login_required(role='Patient')
def patient_token_cancel(token_id):
    user_id = int(session.get('user_id') or 0)
    row = query_db(
        """
        SELECT id, status, token_number
        FROM tokens
        WHERE id=?
          AND created_by_role='PATIENT'
          AND created_by_user_id=?
        """,
        (token_id, user_id),
        one=True,
    )
    if not row:
        return app.response_class(json.dumps({'error': 'not_found'}), status=404, mimetype='application/json')
    if row['status'] not in ('Waiting', 'Generated'):
        return app.response_class(json.dumps({'error': 'cannot_cancel'}), status=400, mimetype='application/json')

    db = get_db()
    db.execute(
        "UPDATE tokens SET status=?, closed_at=? WHERE id=?",
        ('Cancelled', datetime.now().isoformat(), token_id),
    )
    db.commit()
    log_audit_event('PATIENT_TOKEN_CANCELLED', f"token_id={token_id} token={row['token_number']} user_id={user_id}")
    push_event({'type': 'token_cancelled', 'id': token_id, 'created_by_role': 'PATIENT'})
    return app.response_class(json.dumps({'status': 'ok'}), mimetype='application/json')


@app.route('/api/audit_logs')
@login_required(role='Manager')
def api_audit_logs():
    rows = query_db("SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT 50")
    out = [{'id': r['id'], 'event_type': r['event_type'], 'details': r['details'], 'created_at': r['created_at']} for r in rows]
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/generate', methods=['GET', 'POST'])
@login_required(role='Receptionist')
def generate():
    message = None
    token = None
    estimated_wait_min = None
    doctor_queue_len = None
    selected_doctor_id = None
    if request.method == 'POST':
        service = request.form['service']
        selected_doctor_id = int(request.form.get('doctor_id') or 0) or None
        patient_category = _safe_patient_category(request.form.get('patient_category') or 'Regular')
        patient_name = (request.form.get('patient_name') or '').strip() or None
        if (patient_category or '').strip() == 'Emergency':
            recent = query_db('SELECT * FROM tokens ORDER BY generated_time DESC LIMIT 10')
            doctors = get_active_doctors(service)
            return render_template(
                'generate.html',
                message=None,
                token=None,
                recent=recent,
                estimated_wait_min=None,
                doctors=doctors,
                selected_doctor_id=None,
                doctor_queue_len=None,
                error='Emergency cases must be registered via Emergency workflow (not OPD tokens).',
            )
        if not selected_doctor_id:
            recent = query_db('SELECT * FROM tokens ORDER BY generated_time DESC LIMIT 10')
            doctors = get_active_doctors(service)
            return render_template(
                'generate.html',
                message=None,
                token=None,
                recent=recent,
                estimated_wait_min=None,
                doctors=doctors,
                selected_doctor_id=None,
                doctor_queue_len=None,
            )
        # Count tokens for this doctor + service (per-doctor unique numbering)
        cur = get_db().execute(
            "SELECT COUNT(*) AS cnt FROM tokens WHERE doctor_id=? AND service_name=?",
            (selected_doctor_id, service),
        )
        row = cur.fetchone()
        count = row['cnt'] if row else 0
        # Fetch doctor name/initials for token prefix
        doctor_row = query_db("SELECT name FROM doctors WHERE id=?", (selected_doctor_id,), one=True)
        doctor_name = doctor_row['name'] if doctor_row else 'UNKNOWN'
        doctor_code = doctor_name.split()[-1].upper() if doctor_name else 'UNK'  # use last word/initials
        service_code = service[:3].upper()
        token_number = f"{service_code}{count+1}-{doctor_code}"
        generated_time = datetime.now().isoformat()
        assigned_time = generated_time
        base_priority_level = 1 if patient_category == 'Emergency' else 3
        calc = compute_risk_and_priority(
            {
                'patient_category': patient_category,
                'is_followup': 0,
                'doctor_id': selected_doctor_id,
                'service_name': service,
                'generated_time': generated_time,
                'assigned_time': assigned_time,
            }
        )
        db = get_db()
        # Status is "Waiting" for newly generated tokens (matches queue semantics).
        db.execute(
            'INSERT INTO tokens (token_number, service_name, generated_time, assigned_time, doctor_id, created_at, status, is_followup, followup_of_token_id, priority_level, patient_category, risk_score, ai_priority_level, patient_name, created_by_role, created_by_user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (token_number, service, generated_time, assigned_time, selected_doctor_id, generated_time, 'Waiting', 0, None, base_priority_level, patient_category, int(calc['risk_score']), int(calc['ai_priority_level']), patient_name, 'STAFF', int(session.get('user_id') or 0) or None),
        )
        db.commit()
        message = f'Token {token_number} generated'
        token = token_number
        estimated_wait_min = calculate_doctor_estimated_wait_time(selected_doctor_id)
        doctor_queue_len = get_doctor_queue_length(selected_doctor_id)
        # notify connected clients
        push_event({'type': 'token_generated', 'token': token_number, 'service': service})
    recent = query_db('SELECT * FROM tokens ORDER BY generated_time DESC LIMIT 10')
    # Default doctors list based on service (specialization)
    service = request.form.get('service') if request.method == 'POST' else request.args.get('service')
    if not service:
        service = 'Consultation'
    doctors = get_active_doctors(service)
    return render_template(
        'generate.html',
        message=message,
        token=token,
        recent=recent,
        estimated_wait_min=estimated_wait_min,
        doctors=doctors,
        selected_doctor_id=selected_doctor_id,
        doctor_queue_len=doctor_queue_len,
    )


@app.route('/api/doctors')
@login_required()
def api_doctors():
    service = request.args.get('service', '').strip()
    rows = get_active_doctors(service if service else None)
    out = [{'id': int(r['id']), 'name': r['name'], 'specialization': r['specialization']} for r in rows]
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/api/doctor_metrics')
@login_required()
def api_doctor_metrics():
    try:
        doctor_id = int(request.args.get('doctor_id') or 0)
    except Exception:
        doctor_id = 0
    if not doctor_id:
        return app.response_class(json.dumps({'queue_len': None, 'estimated_wait_min': None, 'avg_wait_min': None, 'sla_breach': False}), mimetype='application/json')

    qlen = get_doctor_queue_length(doctor_id)
    est = calculate_doctor_estimated_wait_time(doctor_id)
    avg_wait = calculate_doctor_avg_waiting_time(doctor_id)
    breach = avg_wait is not None and avg_wait > WAITING_SLA
    payload = {
        'queue_len': qlen,
        'estimated_wait_min': est,
        'avg_wait_min': round(avg_wait, 2) if avg_wait is not None else None,
        'sla_breach': breach,
        'sla_min': WAITING_SLA,
    }
    return app.response_class(json.dumps(payload), mimetype='application/json')


@app.route('/api/estimate_wait')
@login_required()
def api_estimate_wait():
    service = request.args.get('service', '').strip()
    if not service:
        return app.response_class(json.dumps({'estimated_wait_min': None}), mimetype='application/json')

    # Estimate for a hypothetical token generated "now".
    # The formula uses tokens_ahead (same service) and avg service time over recent history.
    est = calculate_estimated_wait_time(service, datetime.now().isoformat())
    return app.response_class(json.dumps({'estimated_wait_min': est}), mimetype='application/json')


@app.route('/service_panel')
@login_required(role='Service')
def service_panel():
    tokens = query_db(
        """
        SELECT *
        FROM tokens
        ORDER BY
          CASE
            WHEN status='InProgress' THEN 0
            WHEN status IN ('Waiting','Generated') THEN 1
            ELSE 2
          END,
          priority_level ASC,
          ai_priority_level ASC,
          generated_time ASC
        """
    )
    return render_template('service_panel.html', tokens=tokens)


@app.route('/admin/users', methods=['GET', 'POST'])
@login_required()
@role_required('Admin')
def admin_users():
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add':
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '').strip()
            role = request.form.get('role')
            if username and password and role in ('Admin', 'Receptionist', 'Service'):
                hashed = generate_password_hash(password)
                db = get_db()
                db.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", (username, hashed, role))
                db.commit()
                flash('User added.', 'success')
            else:
                flash('Invalid input.', 'danger')
        elif action == 'delete':
            user_id = request.form.get('user_id')
            # Prevent deleting self
            if user_id and int(user_id) != session.get('user_id'):
                db = get_db()
                db.execute("DELETE FROM users WHERE id = ?", (user_id,))
                db.commit()
                flash('User deleted.', 'success')
            else:
                flash('Cannot delete yourself.', 'danger')
        return redirect(url_for('admin_users'))

    users = query_db("SELECT id, username, role FROM users ORDER BY username")
    return render_template('admin_users.html', users=users)


@app.route('/display')
def display_board():
    # Public display board for waiting room: no login required.
    return render_template('display.html')


@app.route('/display/opd-doctors')
def display_opd_doctors_board():
    # Public wall-display for OPD doctor-wise rotation.
    mode = (request.args.get('mode') or 'carousel').strip().lower()
    if mode not in ('carousel', 'grid'):
        mode = 'carousel'
    return render_template('display_opd_doctors.html', mode=mode)


def _effective_avg_service_min(did: int) -> float:
    avg = calculate_doctor_avg_service_time(did)
    if avg is None:
        avg = calculate_average_service_time(None)
    if avg is None:
        avg = SLA_SERVICE_THRESHOLD_MIN
    try:
        avg = float(avg)
    except Exception:
        avg = float(SLA_SERVICE_THRESHOLD_MIN)
    if avg < 1.0:
        avg = float(SLA_SERVICE_THRESHOLD_MIN)
    return avg


def _doctor_queue_with_eta(doctor_id: int, limit: int = 8):
    now_dt = datetime.now()
    did = int(doctor_id)
    avg = _effective_avg_service_min(did)

    inprog = query_db(
        """
        SELECT id, token_number, patient_name, start_time, patient_category, is_followup, risk_score, ai_priority_level
        FROM tokens
        WHERE doctor_id=?
          AND status='InProgress'
          AND patient_category != 'Emergency'
        ORDER BY start_time ASC
        LIMIT 1
        """,
        (did,),
        one=True,
    )

    available = now_dt
    now_serving = None
    if inprog:
        st = _parse_iso(inprog['start_time'])
        elapsed = _minutes(now_dt - st) if st else 0
        remaining = max(0.0, float(avg) - float(elapsed or 0.0))
        available = now_dt + timedelta(minutes=remaining)
        now_serving = {
            'token_id': int(inprog['id']),
            'token_number': inprog['token_number'],
            'patient_name': inprog['patient_name'] if 'patient_name' in inprog.keys() else None,
            'ai_priority_level': int(inprog['ai_priority_level']) if inprog['ai_priority_level'] is not None else 4,
        }

    waiting = query_db(
        """
        SELECT id, token_number, patient_name, created_at, generated_time, patient_category, is_followup, risk_score, ai_priority_level
        FROM tokens
        WHERE doctor_id=?
          AND status IN ('Waiting','Generated')
          AND patient_category != 'Emergency'
        ORDER BY ai_priority_level ASC, COALESCE(created_at, generated_time) ASC
        LIMIT ?
        """,
        (did, int(limit)),
    )

    queue_out = []
    for r in waiting:
        eta_dt = available if available >= now_dt else now_dt
        queue_out.append({
            'token_id': int(r['id']),
            'token_number': r['token_number'],
            'patient_name': r['patient_name'] if 'patient_name' in r.keys() else None,
            'ai_priority_level': int(r['ai_priority_level']) if r['ai_priority_level'] is not None else 4,
            'is_followup': int(r['is_followup'] or 0),
            'risk_score': int(r['risk_score'] or 0),
            'estimated_start_time': eta_dt.isoformat(),
        })
        available = eta_dt + timedelta(minutes=float(avg))

    return now_serving, queue_out


@app.route('/api/display/opd-doctors')
def api_display_opd_doctors():
    doctors = get_active_doctors()
    out = []
    for d in doctors:
        did = int(d['id'])
        now_serving, q = _doctor_queue_with_eta(did, limit=10)
        out.append({
            'doctor_id': did,
            'doctor_name': d['name'],
            'now_serving': now_serving,
            'queue': q,
        })
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/queue/emergency')
def api_emergency_queue():
    # Emergency workflow is isolated from OPD tokens.
    now_dt = datetime.now()

    active = query_db(
        """
        SELECT ec.*, d.name AS doctor_name
        FROM emergency_cases ec
        LEFT JOIN doctors d ON d.id=ec.assigned_doctor_id
        WHERE ec.status='ACTIVE'
        ORDER BY ec.triage_level ASC, ec.started_at ASC
        LIMIT 10
        """,
    )

    # Now Serving: the most critical (lowest triage number) active case that has a doctor assigned.
    now_serving = None
    for r in active:
        if r['assigned_doctor_id'] is not None:
            now_serving = r
            break

    # Waiting/unassigned: show active cases with no doctor assigned.
    waiting = [r for r in active if r['assigned_doctor_id'] is None]

    # No OPD-based wait estimate; use a simple heuristic for display only.
    est_wait_min = int(len(waiting) * 5)
    overload = _emergency_overload_active()
    state = _get_emergency_state()

    return app.response_class(
        json.dumps({
            'now_serving': {
                'case_id': now_serving['id'],
                'patient_name': now_serving['patient_name'],
                'triage_level': int(now_serving['triage_level']),
                'doctor_id': int(now_serving['assigned_doctor_id']) if now_serving['assigned_doctor_id'] is not None else None,
                'doctor_name': now_serving['doctor_name'] if 'doctor_name' in now_serving.keys() else None,
            } if now_serving else None,
            'waiting_count': len(waiting),
            'estimated_wait_min': est_wait_min,
            'overload': bool(overload),
            'surge_active': bool(state.get('surge_active')),
            'queue': [
                {
                    'case_id': r['id'],
                    'patient_name': r['patient_name'],
                    'triage_level': int(r['triage_level']),
                    'doctor_id': int(r['assigned_doctor_id']) if r['assigned_doctor_id'] is not None else None,
                    'doctor_name': r['doctor_name'] if 'doctor_name' in r.keys() else None,
                    'started_at': r['started_at'],
                }
                for r in waiting
            ],
            'server_time': now_dt.isoformat(),
        }),
        mimetype='application/json',
    )


def _staff_emergency_allowed() -> bool:
    return session.get('role') in ('Receptionist', 'Service', 'Admin', 'Manager')


@app.route('/emergency', methods=['GET'])
@login_required()
def emergency_panel():
    if not _staff_emergency_allowed():
        return redirect(url_for('index'))

    docs = get_emergency_doctors()
    state = _get_emergency_state()
    cases = query_db(
        """
        SELECT ec.*, d.name AS doctor_name
        FROM emergency_cases ec
        LEFT JOIN doctors d ON d.id=ec.assigned_doctor_id
        WHERE ec.status='ACTIVE'
        ORDER BY ec.triage_level ASC, ec.started_at ASC
        LIMIT 20
        """,
    )
    return render_template('emergency_panel.html', cases=cases, emergency_doctors=docs, state=state)


@app.route('/api/emergency/active')
@login_required()
def api_emergency_active():
    if not _staff_emergency_allowed():
        return app.response_class(json.dumps({'error': 'forbidden'}), status=403, mimetype='application/json')
    rows = query_db(
        """
        SELECT ec.*, d.name AS doctor_name
        FROM emergency_cases ec
        LEFT JOIN doctors d ON d.id=ec.assigned_doctor_id
        WHERE ec.status='ACTIVE'
        ORDER BY ec.triage_level ASC, ec.started_at ASC
        LIMIT 50
        """,
    )
    out = []
    for r in rows:
        out.append({
            'id': r['id'],
            'patient_name': r['patient_name'],
            'triage_level': int(r['triage_level']),
            'assigned_doctor_id': int(r['assigned_doctor_id']) if r['assigned_doctor_id'] is not None else None,
            'doctor_name': r['doctor_name'] if 'doctor_name' in r.keys() else None,
            'status': r['status'],
            'started_at': r['started_at'],
            'notes': r['notes'] if 'notes' in r.keys() else None,
        })
    return app.response_class(json.dumps({'cases': out, 'state': _get_emergency_state()}), mimetype='application/json')


@app.route('/emergency/create', methods=['POST'])
@login_required()
@limiter.limit('30/hour')
def emergency_create():
    if not _staff_emergency_allowed():
        return app.response_class(json.dumps({'error': 'forbidden'}), status=403, mimetype='application/json')

    patient_name = (request.form.get('patient_name') or '').strip()
    triage_level = int(request.form.get('triage_level') or 0)
    notes = (request.form.get('notes') or '').strip() or None
    if not patient_name:
        return app.response_class(json.dumps({'error': 'patient_name_required'}), status=400, mimetype='application/json')
    if triage_level not in (1, 2, 3, 4, 5):
        return app.response_class(json.dumps({'error': 'invalid_triage_level'}), status=400, mimetype='application/json')

    assigned = _pick_available_emergency_doctor()
    if triage_level == 1 and assigned is None:
        log_audit_event('EMERGENCY_OVERLOAD', 'triage_level=1 no_available_emergency_doctor')
        push_event({'type': 'EMERGENCY_OVERLOAD'})
    elif assigned is None:
        log_audit_event('EMERGENCY_OVERLOAD', f'triage_level={triage_level} no_available_emergency_doctor')
        push_event({'type': 'EMERGENCY_OVERLOAD'})

    case_id = str(uuid.uuid4())
    now_iso = datetime.now().isoformat()
    db = get_db()
    db.execute(
        """
        INSERT INTO emergency_cases (id, patient_name, triage_level, assigned_doctor_id, status, started_at, closed_at, created_by_user_id, notes)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (case_id, patient_name, int(triage_level), assigned, 'ACTIVE', now_iso, None, int(session.get('user_id') or 0) or None, notes),
    )
    db.commit()
    log_audit_event('EMERGENCY_CREATED', f'id={case_id} triage={triage_level} doctor_id={assigned}')
    push_event({'type': 'EMERGENCY_CREATED', 'id': case_id, 'triage_level': triage_level, 'doctor_id': assigned})
    return app.response_class(json.dumps({'status': 'ok', 'id': case_id, 'assigned_doctor_id': assigned}), mimetype='application/json')


@app.route('/emergency/close/<case_id>', methods=['POST'])
@login_required()
def emergency_close(case_id):
    if not _staff_emergency_allowed():
        return app.response_class(json.dumps({'error': 'forbidden'}), status=403, mimetype='application/json')
    status = (request.form.get('status') or 'COMPLETED').strip().upper()
    if status not in ('COMPLETED', 'TRANSFERRED'):
        status = 'COMPLETED'
    now_iso = datetime.now().isoformat()
    db = get_db()
    db.execute(
        """
        UPDATE emergency_cases
        SET status=?, closed_at=?
        WHERE id=?
          AND status='ACTIVE'
        """,
        (status, now_iso, case_id),
    )
    db.commit()
    log_audit_event('EMERGENCY_CLOSED', f'id={case_id} status={status}')
    push_event({'type': 'EMERGENCY_COMPLETED', 'id': case_id, 'status': status})
    return app.response_class(json.dumps({'status': 'ok'}), mimetype='application/json')


@app.route('/emergency/triage/<case_id>', methods=['POST'])
@login_required(role='Manager')
def emergency_update_triage(case_id):
    # Only manager can modify triage after activation.
    triage_level = int(request.form.get('triage_level') or 0)
    if triage_level not in (1, 2, 3, 4, 5):
        return app.response_class(json.dumps({'error': 'invalid_triage_level'}), status=400, mimetype='application/json')
    db = get_db()
    db.execute(
        "UPDATE emergency_cases SET triage_level=? WHERE id=? AND status='ACTIVE'",
        (int(triage_level), case_id),
    )
    db.commit()
    log_audit_event('EMERGENCY_TRIAGE_UPDATED', f'id={case_id} triage={triage_level}')
    push_event({'type': 'EMERGENCY_UPDATED', 'id': case_id})
    return app.response_class(json.dumps({'status': 'ok'}), mimetype='application/json')


@app.route('/emergency/surge/activate', methods=['POST'])
@login_required(role='Manager')
def emergency_surge_activate():
    state = _get_emergency_state()
    if state.get('surge_active'):
        return app.response_class(json.dumps({'error': 'surge_already_active'}), status=409, mimetype='application/json')

    # Pick one active non-emergency OPD doctor to convert temporarily.
    doc = query_db(
        """
        SELECT id, name
        FROM doctors
        WHERE is_active=1
          AND COALESCE(is_emergency_doctor, 0)=0
        ORDER BY name
        LIMIT 1
        """,
        one=True,
    )
    if not doc:
        return app.response_class(json.dumps({'error': 'no_opd_doctor_available'}), status=400, mimetype='application/json')

    did = int(doc['id'])
    db = get_db()
    db.execute("UPDATE doctors SET is_emergency_doctor=1 WHERE id=?", (did,))
    db.commit()
    now_iso = datetime.now().isoformat()
    _set_emergency_state(1, did, now_iso)
    log_audit_event('EMERGENCY_SURGE_ACTIVATED', f'doctor_id={did}')
    push_event({'type': 'EMERGENCY_SURGE_ACTIVATED', 'doctor_id': did})
    return app.response_class(json.dumps({'status': 'ok', 'doctor_id': did}), mimetype='application/json')


@app.route('/emergency/surge/deactivate', methods=['POST'])
@login_required(role='Manager')
def emergency_surge_deactivate():
    state = _get_emergency_state()
    if not state.get('surge_active'):
        return app.response_class(json.dumps({'error': 'surge_not_active'}), status=409, mimetype='application/json')
    did = state.get('surge_doctor_id')
    if did:
        db = get_db()
        db.execute("UPDATE doctors SET is_emergency_doctor=0 WHERE id=?", (int(did),))
        db.commit()
    _set_emergency_state(0, None, None)
    log_audit_event('EMERGENCY_SURGE_DEACTIVATED', f'doctor_id={did}')
    push_event({'type': 'EMERGENCY_SURGE_DEACTIVATED', 'doctor_id': did})
    return app.response_class(json.dumps({'status': 'ok'}), mimetype='application/json')


@app.route('/api/display_data')
def api_display_data():
    # NOW SERVING: the earliest started in-progress token (if any)
    now_serving = query_db(
        """
        SELECT token_number, service_name, start_time
        FROM tokens
        WHERE status='InProgress'
        ORDER BY start_time ASC
        LIMIT 1
        """,
        one=True,
    )

    def _estimate_upcoming_start_times(waiting_rows):
        now_dt = datetime.now()
        # Track when each doctor is next available.
        doctor_available = {}

        def _effective_avg_service_min(did: int):
            avg = calculate_doctor_avg_service_time(did)
            if avg is None:
                avg = calculate_average_service_time(None)
            if avg is None:
                avg = SLA_SERVICE_THRESHOLD_MIN
            try:
                avg = float(avg)
            except Exception:
                avg = float(SLA_SERVICE_THRESHOLD_MIN)
            # Prevent zero/near-zero averages from collapsing all ETAs to the same timestamp.
            if avg < 1.0:
                avg = float(SLA_SERVICE_THRESHOLD_MIN)
            return avg

        # Seed availability with in-progress remaining time per doctor.
        inprog = query_db(
            """
            SELECT *
            FROM tokens
            WHERE status='InProgress'
              AND doctor_id IS NOT NULL
            """
        )
        for r in inprog:
            did = int(r['doctor_id'])
            avg = _effective_avg_service_min(did)
            st = _parse_iso(r['start_time'])
            elapsed = _minutes(now_dt - st) if st else 0
            remaining = max(0.0, float(avg) - float(elapsed or 0.0))
            doctor_available[did] = now_dt + timedelta(minutes=remaining)

        out = []
        for r in waiting_rows:
            d = dict(r)
            did = d.get('doctor_id')

            # Emergency stays separate conceptually; the display board shows it but time is "Priority".
            is_emergency = False
            try:
                is_emergency = int(d.get('priority_level') or 3) == 1 or _safe_patient_category(d.get('patient_category')) == 'Emergency'
            except Exception:
                is_emergency = False

            eta_iso = None
            if did is None:
                eta_iso = None
            elif is_emergency:
                eta_iso = None
            else:
                did = int(did)
                avg = _effective_avg_service_min(did)
                avail = doctor_available.get(did)
                if avail is None or avail < now_dt:
                    avail = now_dt
                eta_dt = avail
                eta_iso = eta_dt.isoformat()
                doctor_available[did] = eta_dt + timedelta(minutes=float(avg))

            # SLA likely breach (waiting)
            sla_min = SLA_WAIT_THRESHOLD_MIN
            try:
                dept_row = query_db("SELECT sla_threshold_minutes FROM departments WHERE name=?", (d.get('service_name'),), one=True)
                if dept_row and dept_row['sla_threshold_minutes']:
                    sla_min = int(dept_row['sla_threshold_minutes'])
            except Exception:
                pass

            sla_risk = False
            if eta_iso:
                try:
                    eta_dt = datetime.fromisoformat(eta_iso)
                    wait_min = _minutes(eta_dt - now_dt)
                    sla_risk = wait_min is not None and float(wait_min) > float(sla_min)
                except Exception:
                    sla_risk = False

            overload = False
            if d.get('doctor_id') is not None:
                try:
                    overload = bool(doctor_overload_status(int(d.get('doctor_id'))).get('overload'))
                except Exception:
                    overload = False

            out.append({
                'token_number': d.get('token_number'),
                'service_name': d.get('service_name'),
                'doctor_id': d.get('doctor_id'),
                'estimated_start_time': eta_iso,
                'awaiting_assignment': d.get('doctor_id') is None,
                'is_priority': bool(is_emergency),
                'overload': bool(overload),
                'sla_risk': bool(sla_risk),
            })
        return out

    # Fetch more than 5 so we can schedule properly per-doctor and then show first 5.
    waiting = query_db(
        """
        SELECT id, token_number, service_name, generated_time, assigned_time, doctor_id, priority_level, ai_priority_level, patient_category
        FROM tokens
        WHERE status IN ('Waiting','Generated')
        ORDER BY priority_level ASC, ai_priority_level ASC, generated_time ASC
        LIMIT 20
        """
    )
    upcoming_out = _estimate_upcoming_start_times(waiting)[:5]

    # Estimated average waiting time for the room: overall avg service time × queue length
    avg_service_overall = calculate_average_service_time(None)
    waiting_count_row = query_db(
        """
        SELECT COUNT(*) AS cnt
        FROM tokens
        WHERE status IN ('Waiting','Generated')
        """,
        one=True,
    )
    waiting_count = int(waiting_count_row['cnt']) if waiting_count_row else 0
    avg_wait_est = round((avg_service_overall * waiting_count), 1) if avg_service_overall is not None else None

    # SLA status based on today's average waiting/service.
    rows = query_db('SELECT * FROM tokens ORDER BY generated_time DESC')
    analytics = _compute_advanced_analytics(rows)
    sla = analytics.get('sla_status') or check_sla_status(
        analytics.get('avg_wait'),
        analytics.get('avg_service'),
        queue_len=waiting_count,
    )

    payload = {
        'now_serving': {
            'token_number': now_serving['token_number'] if now_serving else None,
            'service_name': now_serving['service_name'] if now_serving else None,
        },
        'upcoming': upcoming_out,
        'estimated_avg_wait_min': avg_wait_est,
        'sla_status': sla,
    }
    return app.response_class(json.dumps(payload), mimetype='application/json')


@app.route('/api/tokens')
@login_required()
def api_tokens():
    rows = query_db(
        """
        SELECT *
        FROM tokens
        ORDER BY
          CASE
            WHEN status='InProgress' THEN 0
            WHEN status IN ('Waiting','Generated') THEN 1
            ELSE 2
          END,
          priority_level ASC,
          ai_priority_level ASC,
          generated_time ASC
        """
    )
    out = []
    for r in rows:
        calc = compute_risk_and_priority(dict(r))
        out.append({
            'id': r['id'],
            'token_number': r['token_number'],
            'service_name': r['service_name'],
            'generated_time': r['generated_time'],
            'start_time': r['start_time'],
            'end_time': r['end_time'],
            'status': r['status'],
            'doctor_id': r['doctor_id'],
            'is_followup': int(r['is_followup']) if 'is_followup' in r.keys() else 0,
            'followup_of_token_id': r['followup_of_token_id'] if 'followup_of_token_id' in r.keys() else None,
            'priority_level': int(r['priority_level']) if 'priority_level' in r.keys() else 3,
            'patient_category': r['patient_category'] if 'patient_category' in r.keys() else 'Regular',
            'risk_score': int(r['risk_score']) if 'risk_score' in r.keys() and r['risk_score'] is not None else int(calc['risk_score']),
            'ai_priority_level': int(r['ai_priority_level']) if 'ai_priority_level' in r.keys() and r['ai_priority_level'] is not None else int(calc['ai_priority_level']),
            'ai_priority_reason': calc.get('reason'),
        })
    return app.response_class(json.dumps(out), mimetype='application/json')


@app.route('/followup', methods=['GET', 'POST'])
@login_required(role='Receptionist')
def register_followup():
    message = None
    error = None
    original = None
    doctors = []
    selected_doctor_id = None

    if request.method == 'POST':
        token_ref = (request.form.get('original_token') or '').strip()
        selected_doctor_id = int(request.form.get('doctor_id') or 0) or None
        if not token_ref:
            error = 'Enter an original token number or ID.'
        else:
            if token_ref.isdigit():
                original = query_db('SELECT * FROM tokens WHERE id=?', (int(token_ref),), one=True)
            else:
                original = query_db('SELECT * FROM tokens WHERE token_number=?', (token_ref,), one=True)

            if not original:
                error = 'Original token not found.'
            else:
                # prevent duplicate follow-up
                dup = query_db(
                    'SELECT id FROM tokens WHERE is_followup=1 AND followup_of_token_id=? LIMIT 1',
                    (original['id'],),
                    one=True,
                )
                if dup:
                    error = 'Follow-up already registered for this token.'
                else:
                    # expiry check
                    base_ts = original['end_time'] or original['generated_time']
                    try:
                        base_dt = datetime.fromisoformat(base_ts)
                    except Exception:
                        base_dt = None
                    if base_dt and (datetime.now() - base_dt).days > FOLLOWUP_MAX_DAYS:
                        error = 'Follow-up window expired. Please generate a new normal token.'
                    else:
                        # continuity doctor
                        orig_doctor_id = original['doctor_id']
                        if not selected_doctor_id:
                            selected_doctor_id = orig_doctor_id

                        # if original doctor inactive, require reassignment
                        doc_row = query_db('SELECT id, is_active, name FROM doctors WHERE id=?', (selected_doctor_id,), one=True)
                        if not doc_row or int(doc_row['is_active']) != 1:
                            # show available doctors for same service
                            doctors = get_active_doctors(original['service_name'])
                            error = 'Original doctor inactive. Please select another doctor.'
                        else:
                            # create follow-up token
                            service = original['service_name']
                            base_token_number = (original['token_number'] or '').strip()
                            token_number = f"{base_token_number}-FU" if base_token_number else None
                            generated_time = datetime.now().isoformat()
                            assigned_time = generated_time
                            patient_category = _safe_patient_category(original['patient_category'] if 'patient_category' in original.keys() else 'Regular')
                            patient_name = original['patient_name'] if 'patient_name' in original.keys() else None
                            calc = compute_risk_and_priority(
                                {
                                    'patient_category': patient_category,
                                    'is_followup': 1,
                                    'doctor_id': selected_doctor_id,
                                    'service_name': service,
                                    'generated_time': generated_time,
                                    'assigned_time': assigned_time,
                                }
                            )
                            db = get_db()
                            db.execute(
                                'INSERT INTO tokens (token_number, service_name, generated_time, assigned_time, doctor_id, created_at, status, is_followup, followup_of_token_id, priority_level, patient_category, risk_score, ai_priority_level, patient_name, created_by_role, created_by_user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                (token_number, service, generated_time, assigned_time, selected_doctor_id, generated_time, 'Waiting', 1, original['id'], 2, patient_category, int(calc['risk_score']), int(calc['ai_priority_level']), patient_name, 'STAFF', int(session.get('user_id') or 0) or None),
                            )
                            db.commit()
                            log_audit_event('FOLLOWUP_REGISTERED', f"Follow-up created for token {original['id']} -> {token_number}")
                            push_event({'type': 'followup_created', 'token': token_number, 'original_token_id': original['id']})
                            message = f'Follow-up token {token_number} registered'

    if not doctors and original:
        doctors = get_active_doctors(original['service_name'])
    return render_template('followup.html', message=message, error=error, original=original, doctors=doctors, selected_doctor_id=selected_doctor_id)


@app.route('/start/<int:token_id>', methods=['POST'])
@login_required(role='Service')
def start_service(token_id):
    start_time = datetime.now().isoformat()
    db = get_db()
    handled_by = session.get('username')
    row = query_db('SELECT id, status, handled_by FROM tokens WHERE id=?', (token_id,), one=True)
    if not row:
        return app.response_class(json.dumps({'error': 'not_found'}), status=404, mimetype='application/json')
    if row['status'] == 'InProgress':
        if row['handled_by'] and row['handled_by'] != handled_by:
            log_audit_event('UNAUTHORIZED_TOKEN_START_ATTEMPT', f"token_id={token_id} by={handled_by} owner={row['handled_by']}")
            return app.response_class(json.dumps({'error': 'forbidden'}), status=403, mimetype='application/json')
        return redirect(url_for('service_panel'))
    db.execute(
        'UPDATE tokens SET start_time=?, called_at=?, handled_by=?, status=? WHERE id=?',
        (start_time, start_time, handled_by, 'InProgress', token_id),
    )
    db.commit()
    push_event({'type': 'service_started', 'id': token_id})
    return redirect(url_for('service_panel'))


@app.route('/end/<int:token_id>', methods=['POST'])
@login_required(role='Service')
def end_service(token_id):
    end_time = datetime.now().isoformat()
    db = get_db()
    # If a token was started in an older version or via a manual update, `handled_by` might be NULL.
    # Set it on completion so agent ranking becomes meaningful.
    handled_by = session.get('username')

    existing = query_db('SELECT id, status, handled_by FROM tokens WHERE id=?', (token_id,), one=True)
    if not existing:
        return app.response_class(json.dumps({'error': 'not_found'}), status=404, mimetype='application/json')
    if existing['status'] != 'InProgress':
        return app.response_class(json.dumps({'error': 'invalid_state'}), status=400, mimetype='application/json')
    if existing['handled_by'] and existing['handled_by'] != handled_by:
        log_audit_event('UNAUTHORIZED_TOKEN_CLOSE_ATTEMPT', f"token_id={token_id} by={handled_by} owner={existing['handled_by']}")
        return app.response_class(json.dumps({'error': 'forbidden'}), status=403, mimetype='application/json')

    current = query_db('SELECT id, doctor_id FROM tokens WHERE id=?', (token_id,), one=True)
    doctor_id = int(current['doctor_id']) if current and current.get('doctor_id') is not None else None

    # Update core fields first
    db.execute(
        """
        UPDATE tokens
        SET end_time=?, closed_at=?, status=?, handled_by=COALESCE(handled_by, ?)
        WHERE id=?
        """,
        (end_time, end_time, 'Completed', handled_by, token_id),
    )
    db.commit()

    # Compute service duration + SLA breach (best-effort)
    row = query_db('SELECT * FROM tokens WHERE id=?', (token_id,), one=True)
    if row:
        st = _parse_iso(row['start_time'])
        et = _parse_iso(row['end_time'])
        sdur = None
        if st and et and et >= st:
            sdur = _minutes(et - st)
        sb = _compute_sla_breached_for_token_row(dict(row))
        try:
            db.execute(
                'UPDATE tokens SET service_duration=?, sla_breached=? WHERE id=?',
                (float(sdur) if sdur is not None else None, int(sb), token_id),
            )
            db.commit()
        except Exception:
            pass

    push_event({'type': 'service_completed', 'id': token_id})

    # Auto-start next token for the same doctor (first waiting in queue)
    try:
        if doctor_id is not None:
            nxt = query_db(
                """
                SELECT id
                FROM tokens
                WHERE doctor_id=?
                  AND status IN ('Waiting','Generated')
                ORDER BY generated_time ASC
                LIMIT 1
                """,
                (doctor_id,),
                one=True,
            )
            if nxt:
                next_id = int(nxt['id'])
                start_time = datetime.now().isoformat()
                db.execute(
                    'UPDATE tokens SET start_time=?, called_at=?, handled_by=?, status=? WHERE id=?',
                    (start_time, start_time, handled_by, 'InProgress', next_id),
                )
                db.commit()
                push_event({'type': 'service_started', 'id': next_id})
    except Exception:
        pass
    return redirect(url_for('service_panel'))


@app.route('/api/admin/mark_followup', methods=['POST'])
@login_required(role='Admin')
def api_admin_mark_followup():
    data = request.get_json(force=True) or {}
    token_id = int(data.get('token_id') or 0)
    followup_required = int(data.get('followup_required') or 0)
    reports_review_required = int(data.get('reports_review_required') or 0)
    if not token_id:
        return app.response_class(json.dumps({'status': 'error', 'message': 'Invalid token_id'}), status=400, mimetype='application/json')
    row = query_db('SELECT id, status, token_number FROM tokens WHERE id=?', (token_id,), one=True)
    if not row:
        return app.response_class(json.dumps({'status': 'error', 'message': 'Token not found'}), status=404, mimetype='application/json')
    if row['status'] != 'Completed':
        return app.response_class(json.dumps({'status': 'error', 'message': 'Token not completed'}), status=400, mimetype='application/json')

    log_audit_event(
        'FOLLOWUP_REQUESTED',
        f"Token {token_id} ({row['token_number']}) followup_required={followup_required} reports_review_required={reports_review_required}",
    )
    push_event({'type': 'followup_requested', 'id': token_id, 'followup_required': followup_required, 'reports_review_required': reports_review_required})
    return app.response_class(json.dumps({'status': 'ok'}), mimetype='application/json')


@app.route('/dashboard')
@login_required(role='Admin')
def dashboard():
    rows = query_db('SELECT * FROM tokens ORDER BY generated_time DESC')
    analytics = _compute_advanced_analytics(rows)
    return render_template(
        'dashboard.html',
        total_today=analytics['total_today'],
        avg_wait=analytics['avg_wait'],
        avg_service=analytics['avg_service'],
        anomalies=analytics['wait_violations'],
        analytics=analytics,
        waiting_sla=WAITING_SLA,
    )


@app.route('/api/dashboard')
@login_required(role='Admin')
def api_dashboard():
    rows = query_db('SELECT * FROM tokens ORDER BY generated_time DESC')
    analytics = _compute_advanced_analytics(rows)
    payload = {
        'total_today': analytics['total_today'],
        'avg_wait': analytics['avg_wait'],
        'avg_service': analytics['avg_service'],
        'anomalies': analytics['wait_violations'],
    }
    return app.response_class(json.dumps(payload), mimetype='application/json')


@app.route('/api/advanced_dashboard')
@login_required(role='Admin')
def api_advanced_dashboard():
    rows = query_db('SELECT * FROM tokens ORDER BY generated_time DESC')
    analytics = _compute_advanced_analytics(rows)
    return app.response_class(json.dumps(analytics), mimetype='application/json')


if __name__ == '__main__':
    # Initialize the SQLite DB and tables if needed.
    init_db()
    _start_risk_reeval_thread_once()
    # Run the app. For production, use a proper WSGI server.
    port = int(os.environ.get('PORT') or 5000)
    debug = (os.environ.get('FLASK_DEBUG') or '').strip() in ('1', 'true', 'True')
    app.run(host='0.0.0.0', port=port, debug=debug)
