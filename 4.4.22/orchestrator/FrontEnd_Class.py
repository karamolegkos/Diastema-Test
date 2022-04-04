import os
import requests

class FrontEnd_Class:
    DIASTEMA_FRONTEND_HOST = os.getenv("DIASTEMA_FRONTEND_HOST", "localhost")
    DIASTEMA_FRONTEND_PORT = int(os.getenv("DIASTEMA_FRONTEND_PORT", 5001))

    def __init__(self):
        self.diastema_front_end_url = "http://"+FrontEnd_Class.DIASTEMA_FRONTEND_HOST+":"+str(FrontEnd_Class.DIASTEMA_FRONTEND_PORT)+"/messages"
        return
    
    def diastema_call(self, message, update = -1, visualization_path = -1, job_name = -1, column = -1):
        url = self.diastema_front_end_url
        form_data = {}

        if(message == "update"):
            form_data = {
                "message": "update",
                "update": update
            }
        elif(message == "visualize"):
            form_data = {
                "message": "visualize",
                "path": visualization_path,
                "job": job_name,
                "column": column
            }
        requests.post(url, form_data)####
        return