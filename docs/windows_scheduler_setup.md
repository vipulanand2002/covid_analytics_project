
# Windows Task Scheduler Setup Instructions

1. Open Task Scheduler (taskschd.msc)
2. Click "Create Basic Task..."
3. Name: "COVID-19 Analytics Pipeline"
4. Trigger: Daily at 6:00 AM
5. Action: Start a program
6. Program: Full path to run_covid_pipeline.bat
7. Start in: Project directory path
8. Finish and test the task

For PowerShell scheduling, use:
Register-ScheduledTask -TaskName "COVID-19-Analytics" -Trigger (New-ScheduledTaskTrigger -Daily -At 6AM) -Action (New-ScheduledTaskAction -Execute "python" -Argument "covid_automation_main.py" -WorkingDirectory "C:\path\to\project")
