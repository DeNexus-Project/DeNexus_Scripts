import time
import subprocess
import sys
import os
import win32serviceutil
import win32service
import win32event
import win32api
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import paths  

class MyPythonService(win32serviceutil.ServiceFramework):
    _svc_name_ = "MyPythonBackgroundService"
    _svc_display_name_ = "My Python Background Service"
    _svc_description_ = "This service runs a Python script periodically in the background."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)

    def SvcDoRun(self):
        while True:
            self.run_task()
            time.sleep(300)  # Wait 5 minutes before running again

    def run_task(self):
        # Run your script
        subprocess.Popen(["python", paths.SCRAP_SCRIPT_PATH])

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(MyPythonService)
