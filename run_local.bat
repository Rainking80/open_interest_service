@echo off
echo Starting Open Interest Service locally...
mkdir data 2>nul
pip install -r requirements.txt
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
pause
