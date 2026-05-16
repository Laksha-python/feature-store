# One‑step startup for the Feature Project demo
# Run this from the project root using PowerShell:
#    .\run_all.ps1

Write-Host "[1/5] Starting Airflow services..."
docker-compose -f .\airflow_orchestration\docker-compose.yml up -d

Write-Host "[2/5] Launching Kafka producer..."
Start-Process powershell -ArgumentList '-NoExit','-Command','python ingestion\kafka_producer.py'

Write-Host "[3/5] Launching raw event consumer..."
Start-Process powershell -ArgumentList '-NoExit','-Command','python ingestion\raw_event_reader.py'

Write-Host "[4/5] Starting API server..."
Start-Process powershell -ArgumentList '-NoExit','-Command','cd api; uvicorn api:app --reload'

Write-Host "[5/5] Starting Streamlit UI..."
Start-Process powershell -ArgumentList '-NoExit','-Command','streamlit run ui/app.py'

Write-Host "All windows are open; monitor their output or close them manually when done."
