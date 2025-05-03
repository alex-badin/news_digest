@echo off
echo Starting script execution at %date% %time%
cd /d "%~dp0"
call C:\Users\user\Documents\ask_media\digest_v0\venv\Scripts\activate.bat
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to activate virtual environment
    exit /b 1
)
python -c "import sys; print(sys.executable)"
"C:\Users\user\Documents\ask_media\digest_v0\venv\Scripts\python.exe" main.py
IF %ERRORLEVEL% NEQ 0 (
    echo Script failed with error code %ERRORLEVEL%
    exit /b 1
)
echo Script completed at %date% %time%