# Import custom Libraries
from normalizing import normalised
from MongoDB_Class import MongoDB_Class
from FrontEnd_Class import FrontEnd_Class

# Import custom Functions for jobs
from data_ingestion import data_ingestion
from cleaning import cleaning
from data_join import data_join

from classification import classification
from regression import regression
from clustering import clustering

from visualize import visualize
from data_sink import data_sink

# Import Libraries
import time
import os

""" Functions used for the json handling """
# Request a job
def job_requestor(job_json, jobs_anwers_dict, playbook):
    """
    A function to handle a Vizualization Job from the Diastema JSON playbook.

    Args:
        - job_json (JSON): The job to request to be done.
        - jobs_anwers_dict (Dictionary): A dictionary holding all the return values of every 
            Diastema job done in the given analysis so far.
        - playbook (JSON): The Diastema playbook.

    Returns:
        - Nothing.
    """
    title = job_json["title"]
    step = job_json["step"]
    from_step = job_json["from"]
    
    if(title == "dataset"):
        print("[INFO] Dataset Found.")
        jobs_anwers_dict[step] = normalised(playbook["database-id"])+"/datasets/"+normalised(job_json["label"])
    
    if(title == "cleaning"):
        print("[INFO] Cleaning Found.")
        jobs_anwers_dict[step] = cleaning(playbook, job_json, jobs_anwers_dict[from_step], max_shrink = job_json["max-shrink"])
    
    if(title == "classification"):
        print("[INFO] Classification Found.")
        jobs_anwers_dict[step] = classification(playbook, job_json, jobs_anwers_dict[from_step], algorithm = job_json["algorithm"])
    
    if(title == "regression"):
        print("[INFO] Regression Found.")
        jobs_anwers_dict[step] = regression(playbook, job_json, jobs_anwers_dict[from_step], algorithm = job_json["algorithm"])
    
    if(title == "clustering"):
        print("[INFO] Clustering Found.")
        jobs_anwers_dict[step] = clustering(playbook, job_json, jobs_anwers_dict[from_step], algorithm = job_json["algorithm"])
    
    if(title == "visualize"):
        print("[INFO] Visualization Found.")
        jobs_anwers_dict[step] = visualize(playbook, job_json, jobs_anwers_dict[from_step])
    
    if(title == "data-sink"):
        print("[INFO] Data-Sink Found.")
        jobs_anwers_dict[step] = data_sink(playbook, job_json, jobs_anwers_dict[from_step])
    
    if(title == "data-join"):
        print("[INFO] Data Join Found.")
        jobs_anwers_dict[step] = data_join(playbook, job_json, jobs_anwers_dict[from_step[0]], jobs_anwers_dict[from_step[1]])

    return

# Access jobs by viewing them Depth-first O(N)
def jobs(job_step, jobs_dict, jobs_anwers_dict, playbook, joins):
    """
    A Depth first recursive function, running every job of the Diastema analysis.

    Args:
        - job_step (Integer): The step of the job to parse.
        - jobs_dict (Dictionary): A Dictionary with every job from the requests.
        - jobs_anwers_dict (Dictionary): A dictionary holding all the return values of every 
            Diastema job done in the given analysis so far.
        - playbook (JSON): The Diastema playbook.

    Returns:
        - Nothing.
    """
    flagged = False
    if(jobs_dict[job_step]["title"] == "data-join" and not(job_step in joins)):
        # If data-join + first time seen => do not go deeper
        # But keep the join as seen 
        joins[job_step] = True
        flagged = True
    else:
        job_requestor(jobs_dict[job_step], jobs_anwers_dict, playbook)

    
    # Depth-first approach
    next_steps = jobs_dict[job_step]["next"]
    for step in next_steps:
        if(step == 0): # If ther is no next job then do not try to go deeper
            pass
        elif(flagged == True): # If this job is flagged do not try to go deeper
            pass
        else:
            jobs(step, jobs_dict, jobs_anwers_dict, playbook, joins)
            
    return

# Handle the playbook
def handler(playbook):
    """
    A function to handle and run the Diastema playbook.

    Args:
        - playbook (JSON): The Diastema playbook.

    Returns:
        - Nothing.
    """
    print("[INFO] Finding starting jobs - Datasets.")
    # The jobs of the playbook.
    json_jobs = playbook["jobs"]

    # handle jobs as a dictionary - O(N)
    jobs_dict = {}
    for job in json_jobs:
        jobs_dict[job["step"]] = job
    
    # Find starting jobs - O(N)
    starting_jobs = []
    for job_step, job in jobs_dict.items():
        # print(job_step, '->', job)
        if job["from"] == 0:
            starting_jobs.append(job_step)
    #print(starting_jobs)
    
    print("[INFO] Starting Jobs Found.")

    # Use a dictionary as a storage for each job answer
    jobs_anwers_dict = {}
    joins = {}
    
    # for each starting job, start the analysis
    print("[INFO] Starting the Depth-First Algorithm.")
    for starting_job_step in starting_jobs:
        job = jobs_dict[starting_job_step]
        # navigate through all the jobs and execute them in the right order
        jobs(starting_job_step, jobs_dict, jobs_anwers_dict, playbook, joins)
    
    # Print jobs_anwers_dict for testing purposes
    for job_step, answer in jobs_anwers_dict.items():
        print("[INFO]", job_step, '->', answer)
    
    return

# A function called in a new Thread to execute the analysis
def analysis_thread(playbook):
    print("[INFO] Starting handling the analysis given.")

    # Send the playbook for handling
    handler(playbook)

    # Insert metadata in mongo
    print("[INFO] Inserting analysis metadata in mongoDB.")
    mongo_obj = MongoDB_Class()
    metadata_json = playbook["metadata"]
    metadata_record = {"kind":"metadata", "metadata":metadata_json}
    mongo_obj.insertMongoRecord(normalised(playbook["database-id"]), "analysis_"+normalised(playbook["analysis-id"]), metadata_record)
    print("[INFO] Metadata Inserted.")

    # Contact front end for the ending of the analysis
    print("[INFO] Contacting User for the ending of an analysis.")
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "update", update = ("Analysis completed with ID: "+normalised(playbook["analysis-id"])))
    print("[INFO] User contacted.")
    return