# Import custom Libraries
from normalizing import normalised

from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class
from FrontEnd_Class import FrontEnd_Class
from Kubernetes_Class import Kubernetes_Class

# Import Libraries
import io

def regression(playbook, job, last_bucket, algorithm=False, tensorfow_algorithm=False):
    algorithms = {
        "linear regression" : False,                    # Not imported yet
        "generalized linear regression" : False,        # Not imported yet
        "decision tree regression" : "decisiontree",
        "random forest regression" : "randomforest",
        "gradient-boosted tree regression" : False      # Not imported yet
    }

    algorithm_to_use = ""
    default_job = "decision tree regression"
    if algorithm==False:
        algorithm_to_use = algorithms[default_job]
    else:
        if algorithm in algorithms:
            algorithm_to_use = algorithms[algorithm]
            if algorithm_to_use == False:
                algorithm_to_use = algorithms[default_job]
        else:
            algorithm_to_use = algorithms[default_job]
    
    # Path of regression in Diastema docker image
    analysis_path = "/app/src/RegressionJob.py"

    # Data Bucket = last jobs output bucket
    data_bucket = last_bucket

    # Analysis Bucket = User/analysis-id/job-step
    analysis_bucket = normalised(playbook["database-id"])+"/analysis-"+normalised(playbook["analysis-id"])+"/regressed-"+normalised(job["step"])

    # Jobs arguments
    job_args = [analysis_path, algorithm_to_use, data_bucket, analysis_bucket, job["column"]]

    # Make the MinIO Analysis buckers
    minio_obj = MinIO_Class()
    minio_obj.put_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/regressed-"+normalised(job["step"])+"/", io.BytesIO(b""), 0,)

    # Make the Spark call
    spark_call_obj = Kubernetes_Class()
    spark_call_obj.spark_caller(job_args)

    # Remove the _SUCCESS file from the  spark job results
    minio_obj.remove_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/regressed-"+normalised(job["step"])+"/_SUCCESS")

    # Insert the regressed data in MongoDB
    regression_job_record = {"minio-path":analysis_bucket, "directory-kind":"regressed-data", "job-json":job}

    mongo_obj = MongoDB_Class()
    mongo_obj.insertMongoRecord(normalised(playbook["database-id"]), "analysis_"+normalised(playbook["analysis-id"]), regression_job_record)

    # Contact front end for the ending of the job
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = "Regression executed for the analysis with ID: "+playbook["analysis-id"])

    # Return the bucket that this job made output to 
    return analysis_bucket