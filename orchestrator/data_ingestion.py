# Import custom Libraries
from normalizing import normalised

from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class
from FrontEnd_Class import FrontEnd_Class
from Diastema_Service import Diastema_Service

# Import Libraries
import io

def data_ingestion(playbook, job):
    # Bucket to Ingest inside Data
    load_bucket = normalised(playbook["database-id"])+"/analysis-"+normalised(playbook["analysis-id"])+"/ingested-"+normalised(job["step"])

    # Make the load Bucket directory
    minio_obj = MinIO_Class()
    minio_obj.make_bucket(normalised(playbook["database-id"]))
    minio_obj.put_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/ingested-"+normalised(job["step"])+"/", io.BytesIO(b""), 0,)

    # Make websocket call for the Data Loading Service
    extracting_info = {
        "minio-output" : load_bucket, 
        "job-id" : normalised(job["id"]),
        "url" : job["link"],
        "method" : job["method"],
        "token" : job["token"]
    }

    # Start Loading Service
    service_obj = Diastema_Service()
    service_obj.startService("data-ingesting", extracting_info)

    # Wait for loading to End
    service_obj.waitForService("data-ingesting", job["id"])

    # Insert the raw and loaded data in MongoDB
    data_load_job_record = {"minio-path":load_bucket, "directory-kind":"ingested-data", "job-json":job}

    mongo_obj = MongoDB_Class()
    mongo_obj.insertMongoRecord(normalised(playbook["database-id"]), "analysis_"+normalised(playbook["analysis-id"]), data_load_job_record)

    # Contact front end for the ending of the job
    # diastema_call(minioString(playbook["database-id"]), "analysis-"+minioString(playbook["analysis-id"]), "data-load")
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = ("Ingested Dataset with ID: "+normalised(job["id"])))

    # Return the bucket that this job made output to
    return load_bucket