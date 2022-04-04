# Import custom Libraries
from normalizing import normalised

from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class
from FrontEnd_Class import FrontEnd_Class
from Diastema_Service import Diastema_Service

# Import Libraries
import io

def data_join(playbook, job, last_bucket_1, last_bucket_2):
    # get the new bucket
    joined_bucket = normalised(playbook["database-id"])+"/analysis-"+normalised(playbook["analysis-id"])+"/joined-"+normalised(job["step"])

    # Make the MinIO Join bucket
    minio_obj = MinIO_Class()
    minio_obj.put_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/joined-"+normalised(job["step"])+"/", io.BytesIO(b""), 0,)

    # Get the needed attributes
    join_data = {
        "job-id" : normalised(job["id"]),
        "column" : normalised(job["column"]),
        "type" : normalised(job["join-type"]),
        "inputs" : [last_bucket_1, last_bucket_2],
        "output" : joined_bucket
    }

    # Start Loading Service
    service_obj = Diastema_Service()
    service_obj.startService("join", join_data)

    # Wait for loading to End
    service_obj.waitForService("join", job["id"])

    # Insert the cleaned data in MongoDB
    joined_job_record = {"minio-path":joined_bucket, "directory-kind":"joined-data", "job-json":job}

    mongo_obj = MongoDB_Class()
    mongo_obj.insertMongoRecord(normalised(playbook["database-id"]), "analysis_"+normalised(playbook["analysis-id"]), joined_job_record)

    # Contact front end for the ending of the job
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = "Join executed.")

    # Return the bucket that this job made output to
    return joined_bucket