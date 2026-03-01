# Service Token Management and Analysis System for Healthcare

A beginner-friendly Flask web application that demonstrates a simple
Service Token Management and Analysis System tailored for healthcare.

Purpose
- Generate and manage service tokens for hospital workflows (Consultation, Lab Test).
- Track generated, in-progress, and completed tokens with timestamps.
- Provide role-based access (Admin, Receptionist, Service) and basic analytics.

Key Features
- User registration and login with roles stored in SQLite.
- Receptionist: generate tokens (auto-increment per day, e.g. CON-1).
- Service staff: start and end services (record start_time and end_time).
- Admin dashboard: total tokens today, average waiting time, average service time, anomaly highlighting (waiting > 20 minutes).
- Public display board (no login) to show “Now Serving” and next tokens.
- Admin user management (create/delete users).
- Real-time updates using Server-Sent Events (SSE) and AJAX for incremental UI refresh.

Project files
- `app.py` : main Flask app (routes, DB init, SSE, JSON APIs)
- `templates/` : HTML templates (login, register, generate, service_panel, dashboard, base)
- `tokens.db` : SQLite database (created automatically in project root)
- `requirements.txt` : required Python packages

Main routes
- `/login` : login page
- `/register` : register page
- `/generate` : receptionist token generator
- `/service_panel` : service staff panel (start/end tokens)
- `/dashboard` : admin analytics dashboard
- `/admin/users` : admin user management
- `/display` : public display board (no login)

JSON/API routes
- `/api/display_data` : data for the public display board (now serving + upcoming)
- `/api/estimate_wait?service=...` : estimated waiting time for a service
- `/api/advanced_dashboard` : dashboard live data (violations/rankings)

Database schema (SQLite)
- `users` table: (id, username, password, role)
  - Stores users and their role (Admin, Receptionist, Service).
- `tokens` table: (id, token_number, service_name, generated_time, start_time, end_time, status)
  - Stores each token and timestamps. `status` is one of `Generated`, `InProgress`, `Completed`.

Sample accounts (auto-created)
- Admin: username `admin`, password `password`
- Receptionist: username `reception`, password `password`
- Service: username `service`, password `password`

How it works (brief)
- Receptionist generates a token → saved to `tokens` table with `status='Generated'` and `generated_time`.
- Service staff sees tokens in the Service Panel, clicks Start → `start_time` and `status='InProgress'` saved.
- Service staff clicks End → `end_time` and `status='Completed'` saved.
- Admin opens Dashboard to view metrics computed from today's tokens.
- SSE pushes allow the Service Panel and Dashboard to update automatically when token events occur.

Run instructions (Windows)
1. From the project folder:

```powershell
cd D:\ISE\STMA
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

2. Open a browser at: `http://127.0.0.1:5000`

Quick demo script for judges (2–3 minutes)
1. Open browser → `http://127.0.0.1:5000` (Login page).
2. Login as Receptionist: `reception` / `password` → go to Token Generator.
3. Generate a token for Consultation → note token number and time.
4. Open a new incognito window, login as Service: `service` / `password` → open Service Panel.
   - Confirm the generated token appears (real-time).
   - Click Start → verify StartTime recorded; click End → verify EndTime recorded.
5. Login as Admin: `admin` / `password` → open Dashboard and show metrics and anomalies.
6. Open public board: `http://127.0.0.1:5000/display` to show “Now Serving” + next tokens.

Showing the database to the judges
- The SQLite file `tokens.db` is created in the project root (the folder where you run `python app.py`).

Useful commands (PowerShell):
```powershell
# show the DB file
Get-ChildItem -Path . -Filter tokens.db -Recurse
Resolve-Path tokens.db

# show tables
sqlite3 tokens.db ".tables"

# show tokens schema
sqlite3 tokens.db "PRAGMA table_info(tokens);"

# view some rows
sqlite3 tokens.db "SELECT * FROM tokens LIMIT 10;"

# print DB info with Python (if sqlite3 CLI not available)
python - <<'PY'
import sqlite3, os
db='tokens.db'
print('DB path:', os.path.abspath(db))
conn=sqlite3.connect(db)
for row in conn.execute("SELECT * FROM tokens LIMIT 5"):
    print(row)
conn.close()
PY
```

- Compress the DB for handover:
```powershell
Compress-Archive -Path tokens.db -DestinationPath tokens.zip
```
- Or open `tokens.db` with a GUI (DB Browser for SQLite) for an interactive demonstration.

Possible next enhancements (optional)
- Add Admin UI to manage users and tokens.
- Add CSV export for dashboard metrics.
- Improve authentication (stronger secret, session security) for production.

