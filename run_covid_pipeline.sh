#!/bin/bash
echo "Starting COVID-19 Analytics Pipeline..."
cd "$(dirname "$0")"
python3 covid_automation_main.py
if [ $? -eq 0 ]; then
    echo "Pipeline completed successfully"
else
    echo "Pipeline failed with error code $?"
fi
