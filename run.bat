@echo off
REM --- SET ENVIRONMENT VARIABLES ---
REM **CRITICAL:** REPLACE THE PATH BELOW WITH YOUR ACTUAL JAVA JDK INSTALLATION PATH
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_401 

REM Set SPARK_HOME to a common path or leave empty if PySpark auto-downloads
REM If you did not install Spark binaries manually, you can often omit SPARK_HOME or point it to a dummy path
REM set SPARK_HOME=C:\spark\spark-3.4.2-bin-hadoop3.3

REM --- ACTIVATE VENV AND RUN ---
call venv\Scripts\activate.bat

echo Installing dependencies...
pip install -r requirements.txt

echo Running PySpark Analysis (SDG 10)...
python run_analysis.py

echo.
echo Analysis Complete. Check the 'results' folder.

pause