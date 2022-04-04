import requests
import os
import time

class Diastema_Service:
    DIASTEMA_SERVICES_HOST = os.getenv("DIASTEMA_SERVICES_HOST", "localhost")
    DIASTEMA_SERVICES_PORT = int(os.getenv("DIASTEMA_SERVICES_PORT", 5001))

    def __init__(self):
        self.diastema_services_url = "http://"+Diastema_Service.DIASTEMA_SERVICES_HOST+":"+str(Diastema_Service.DIASTEMA_SERVICES_PORT)+"/"
        pass
    
    def startService(self, service_name, json_body):
        url = self.diastema_services_url+service_name
        requests.post(url, json=json_body)
        return
    
    def waitForService(self, service_name, job_id):
        url = self.diastema_services_url+service_name+"/progress?id="+str(job_id)
        responce = requests.get(url)
        while True:
            time.sleep(2)
            if(responce.text == "complete"):
                break
            responce = requests.get(url)
        return
    
    def getServiceResults(self, service_name, job_id):
        url = self.diastema_services_url+service_name+"/"+str(job_id)
        responce = requests.get(url)
        # Future use
        return