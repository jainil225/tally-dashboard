# Tally Dashboard

A real-time Tally Prime financial dashboard built with Flask + PostgreSQL (Neon).

## 🚀 Deploy on Render

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) → New → Web Service
3. Connect your GitHub repo
4. Render auto-detects `render.yaml` — just click **Deploy**

## 💻 Run Locally

```bash
pip install -r requirements.txt
python server.py
```
Open: http://localhost:8080

## 📁 Files

| File | Purpose |
|------|---------|
| `index.html` | Full dashboard UI (HTML + CSS + JS) |
| `server.py` | Flask backend with all API routes |
| `render.yaml` | Render deployment config |
| `requirements.txt` | Python dependencies |
| `_python-version` | Python 3.11.4 pin |
| `start.bat` | Windows one-click launcher |

## ✨ Features

- Sales & Purchase trend charts
- Receivables aging with drill-down
- Day Book with date picker
- Top Customers & Suppliers
- Cash / Bank account summary
- Dark / Light mode toggle
- AI Financial Assistant (Groq — bring your own key)
- Company switcher (auto-loads from DB)

## 🔑 AI Chat Setup

Get a free API key at [console.groq.com](https://console.groq.com) and paste it in the 🤖 AI panel.
