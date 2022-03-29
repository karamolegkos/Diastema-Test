# Import custom Libraries
from normalizing import normalised

from FrontEnd_Class import FrontEnd_Class
from Diastema_Service import Diastema_Service

def data_sink(playbook, job, last_bucket):
    input_path = last_bucket

    form_data = {"minio-input": input_path, "kafka-message": "TO BE UPDATED", "job-id":normalised(job["id"])}

    # Start Sink Service
    service_obj = Diastema_Service()
    service_obj.startService("data-sink", form_data)

    # Wait for Sink sending to End
    service_obj.waitForService("data-sink", job["id"])

    # Contact front end
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = "Data are now in the data sink.")
    
    # dummy return
    return "data-output-from: "+last_bucket