@echo off
title Tally Dashboard
cd /d "%~dp0"
echo Installing packages...
python -m pip install flask flask-cors psycopg2-binary --quiet
echo Starting dashboard...
echo Open: http://localhost:8080
python server.py
pause
