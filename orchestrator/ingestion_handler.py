# Import custom Libraries
from MongoDB_Class import MongoDB_Class

# Import custom Functions for jobs
from data_ingestion import data_ingestion

# A function called in a new Thread to execute the ingestion
def ingestion_thread(playbook):
    print("[INFO] Starting handling the ingestion given.")

    # Do the ingestion using Diastema Services and get the features of the Dataset
    features = data_ingestion(playbook)

    # Updated the MongoDB record of the Dataset
    print("[INFO] Inserting ingestion features in mongoDB.")
    mongo_obj = MongoDB_Class()
    filters = {"organization": playbook["database-id"], "user": playbook["user"], "label": playbook["dataset-label"]}
    mongo_obj.updateMongoFeatures("UIDB", "datasets", filters, features)

    return