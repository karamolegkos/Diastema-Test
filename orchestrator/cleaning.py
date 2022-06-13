# Import custom Libraries
from normalizing import normalised

from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class
from FrontEnd_Class import FrontEnd_Class
from Diastema_Service import Diastema_Service

# Import Libraries
import io

def cleaning(playbook, job, last_bucket, max_shrink=False, json_schema=False):
    # Data Bucket = last jobs output bucket
    data_bucket = last_bucket

    # Analysis Bucket = User/analysis-id/job-step
    analysis_bucket = normalised(playbook["database-id"])+"/analysis-"+normalised(playbook["analysis-id"])+"/cleaned-"+normalised(job["step"])

    # Jobs arguments
    #job_args = ["/root/spark-job/cleaning-job.py", data_bucket, analysis_bucket]

    # Make the MinIO Analysis buckers
    minio_obj = MinIO_Class()
    minio_obj.put_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/cleaned-"+normalised(job["step"])+"/", io.BytesIO(b""), 0,)

    # Make the websocket call for the Data Cleaning Service
    form_data = {"minio-input": data_bucket, "minio-output": analysis_bucket, "job-id":normalised(job["id"])}
    # Optional attr max-shrink
    if max_shrink != False:
        form_data["max-shrink"] = max_shrink

    # Make websocket call for the Data Loading Service
    cleaning_info = form_data

    # Start Loading Service
    service_obj = Diastema_Service()
    service_obj.startService("data-cleaning", cleaning_info)

    # Wait for loading to End
    service_obj.waitForService("data-cleaning", job["id"])

    # Insert the cleaned data in MongoDB
    cleaning_job_record = {"minio-path":analysis_bucket, "directory-kind":"cleaned-data", "job-json":job}

    mongo_obj = MongoDB_Class()
    mongo_obj.insertMongoRecord(normalised(playbook["database-id"]), "analysis_"+normalised(playbook["analysis-id"]), cleaning_job_record)

    # Contact front end for the ending of the job
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = "Cleaning executed for the analysis with ID: "+playbook["analysis-id"])

    # Return the bucket that this job made output to 
    return analysis_bucket