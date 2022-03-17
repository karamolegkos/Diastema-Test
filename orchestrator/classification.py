# Import custom Libraries
from normalizing import normalised

from MongoDB_Class import MongoDB_Class
from MinIO_Class import MinIO_Class
from FrontEnd_Class import FrontEnd_Class
from Kubernetes_Class import Kubernetes_Class

# Import Libraries
import io

def classification(playbook, job, last_bucket, algorithm=False, tensorfow_algorithm=False):
    algorithms = {
        "logistic regression" : False,                  # Not imported yet
        "decision tree classifier" : "decisiontree",
        "random forest classifier" : "randomForest",
        "gradient-boosted tree classifier" : False,     # Not imported yet
        "multilayer perceptron classifier" : False,     # Not imported yet
        "linear support vector machine" : False,        # Not imported yet
        "support vector machine" : False                # Not imported yet
    }

    algorithm_to_use = ""
    default_job = "decision tree classifier"
    if algorithm==False:
        algorithm_to_use = algorithms[default_job]
    else:
        if algorithm in algorithms:
            algorithm_to_use = algorithms[algorithm]
            if algorithm_to_use == False:
                algorithm_to_use = algorithms[default_job]
        else:
            algorithm_to_use = algorithms[default_job]
    
    # Path of classification in Diastema docker image
    analysis_path = "/app/src/ClassificationJob.py"

    # Data Bucket = last jobs output bucket
    data_bucket = last_bucket

    # Analysis Bucket = User/analysis-id/job-step
    analysis_bucket = normalised(playbook["database-id"])+"/analysis-"+normalised(playbook["analysis-id"])+"/classified-"+normalised(job["step"])

    # Jobs arguments
    job_args = [analysis_path, algorithm_to_use, data_bucket, analysis_bucket, job["column"]]

    # Make the MinIO Analysis buckers
    minio_obj = MinIO_Class()
    minio_obj.put_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/classified-"+normalised(job["step"])+"/", io.BytesIO(b""), 0,)

    # Make the Spark call
    spark_call_obj = Kubernetes_Class()
    spark_call_obj.spark_caller(job_args)
    
    # Remove the _SUCCESS file from the  spark job results
    minio_obj.remove_object(normalised(playbook["database-id"]), "analysis-"+normalised(playbook["analysis-id"])+"/classified-"+normalised(job["step"])+"/_SUCCESS")
    
    # Insert the classified data in MongoDB
    classification_job_record = {"minio-path":analysis_bucket, "directory-kind":"classified-data", "job-json":job}

    mongo_obj = MongoDB_Class()
    mongo_obj.insertMongoRecord(normalised(playbook["database-id"]), "analysis_"+normalised(playbook["analysis-id"]), classification_job_record)

    # Contact front end for the ending of the job
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = "Classification executed.")

    # Return the bucket that this job made output to 
    return analysis_bucket