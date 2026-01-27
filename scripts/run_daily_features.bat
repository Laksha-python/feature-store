@echo off
cd /d C:\Feature Project
python ingestion\fake_event_generator.py >> logs\feature_job.log 2>&1
