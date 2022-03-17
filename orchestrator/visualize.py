# Import custom Libraries
from FrontEnd_Class import FrontEnd_Class

def visualize(playbook, job, last_bucket):
    # The Data to be visualised are saved in the bucket below
    visualization_path = last_bucket

    # Get the job kind based in the Diastema JSON playbook
    broke_bucket = visualization_path.split("/")
    job_done = broke_bucket[2].split("-")[0]
    job_kind = ""
    if(job_done == "loaded"):
        job_kind = "data-load"
    elif(job_done == "cleaned"):
        job_kind = "cleaning"
    elif(job_done == "classified"):
        job_kind = "classification"
    elif(job_done == "regressed"):
        job_kind = "regression"
    else:
        job_kind = "clustering"
    
    # get the step that the visualization Job came from
    step_to_visualize = job["from"]

    # Use all the playbook to find the last job
    jobs = playbook["jobs"]

    # the column to be visualized
    vis_column = ""

    for i_job in jobs:
        if(i_job["step"] == step_to_visualize):
            vis_column = i_job["column"].lower()
            break

    # Contact front end to make a visualization
    front_obj = FrontEnd_Class()
    front_obj.diastema_call(message = "visualize", visualization_path = last_bucket, job_name = job_kind, column = vis_column)
    
    # dummy return
    return "visualization-from: "+last_bucket