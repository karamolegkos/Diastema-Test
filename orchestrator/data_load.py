# Import custom Libraries
from normalizing import normalised

from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class
from FrontEnd_Class import FrontEnd_Class
from Diastema_Service import Diastema_Service

# Import Libraries
import io

def data_load(playbook, job, last_bucket):
    # Raw bucket = User/analysis-id/job(-id) - ID will be included in later updates
    # raw_bucket = minioString(playbook["database-id"])+"/analysis-"+minioString(playbook["analysis-id"])+"/raw-"+minioString(job["id"])
    raw_bucket = last_bucket

    # Bucket to Load Data = User/analysis-id/job-step
    load_bucket = normalised(playbook["database-id"])+"/analysis-"+normalised(playbook["analysis-id"])+"/loaded-"+normalised(job["step"])

    # Make the load Bucket directory
    minio_obj = MinIO_Class()
    minio_obj.put_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/loaded-"+normalised(job["step"])+"/", io.BytesIO(b""), 0,)

    # Make websocket call for the Data Loading Service
    loading_info = {"minio-input": raw_bucket, "minio-output": load_bucket, "job-id":normalised(job["id"])}

    # Start Loading Service
    service_obj = Diastema_Service()
    service_obj.startService("data-loading", loading_info)

    # Wait for loading to End
    service_obj.waitForService("data-loading", job["id"])

    # Insert the raw and loaded data in MongoDB
    data_load_job_record = {"minio-path":load_bucket, "directory-kind":"loaded-data", "job-json":job}

    mongo_obj = MongoDB_Class()
    mongo_obj.insertMongoRecord(normalised(playbook["database-id"]), "analysis_"+normalised(playbook["analysis-id"]), data_load_job_record)

    # Contact front end for the ending of the job
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = "Loaded Dataset for the analysis with ID: "+playbook["analysis-id"])

    # Return the bucket that this job made output to
    return load_bucket