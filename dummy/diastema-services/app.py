from flask import Flask, request, Response, jsonify
import json
import os
import logging
# For dummy results
from minio import Minio
import time
import io
import random

MINIO_HOST = os.getenv("MINIO_HOST", "localhost")
MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
MINIO_USER = os.getenv("MINIO_USER", "diastema")
MINIO_PASS = os.getenv("MINIO_PASS", "diastema")

# MinIO HOST and Client
minio_host = MINIO_HOST+":"+str(MINIO_PORT)
minio_client = Minio(
        minio_host,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )

# Debugging
Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "debug.log",
                    filemode = "w",
                    format = Log_Format, 
                    level = logging.DEBUG)

logging.info("Server Started")

# Flask app
app = Flask(__name__)

""" Flask endpoints - Dummy Front End Endpoints """
# A dummy endpoint to represent the answer of the front end services
@app.route("/messages", methods=["POST"])
def modelling():
    message = request.form["message"]
    if(message == "update"):
        logging.info(request.form["message"])
        logging.info(request.form["update"])
    elif(message == "visualize"):
        logging.info(request.form["message"])
        #print(request.form["path"])
        #print(request.form["job"])
        #print(request.form["column"])
    return Response(status=200, mimetype='application/json')

@app.route("/join", methods=["POST"])
def data_loading_join():
    json_body = request.json
    logging.info("Joining Given JSON")
    logging.debug(json.dumps(json_body))
    time.sleep(2)

    json_attrs = json_body
    minio_path  = json_attrs["output"].split("/")
    minio_bucket = minio_path[0]
    del minio_path[0]
    minio_object = '/'.join([str(elem) for elem in minio_path])
    minio_results_object = minio_object + "/joined.txt"
    minio_client.put_object(minio_bucket, minio_results_object, io.BytesIO(b"results"), 7)

    logging.info("Joining Done")

    return Response(status=200, mimetype='application/json')

@app.route("/data-cleaning", methods=["POST"])
def data_cleaning():
    json_body = request.json
    logging.info("Loading Given JSON")
    logging.debug(json.dumps(json_body))
    time.sleep(2)

    json_attrs = json_body
    minio_path  = json_attrs["minio-output"].split("/")
    minio_bucket = minio_path[0]
    del minio_path[0]
    minio_object = '/'.join([str(elem) for elem in minio_path])
    minio_results_object = minio_object + "/cleaned.txt"
    minio_client.put_object(minio_bucket, minio_results_object, io.BytesIO(b"results"), 7)

    logging.info("Cleaning Done")
    return Response(status=200, mimetype='application/json')

@app.route("/data-ingesting", methods=["POST"])
def data_ingesting():
    json_body = request.json
    logging.info("Ingesting data")
    logging.debug(json.dumps(json_body))
    time.sleep(2)

    json_attrs = json_body
    minio_path  = json_attrs["minio-output"].split("/")
    minio_bucket = minio_path[0]
    del minio_path[0]
    minio_object = '/'.join([str(elem) for elem in minio_path])
    minio_results_object = minio_object + "/ingested.txt"
    minio_client.put_object(minio_bucket, minio_results_object, io.BytesIO(b"results"), 7)

    logging.info("Ingestion Done")
    return Response(status=200, mimetype='application/json')

@app.route("/data-sink", methods=["POST"])
def data_sending():
    json_body = request.json
    logging.info("Sending data to Kafka")
    logging.debug(json.dumps(json_body))
    time.sleep(2)
    logging.info("Sending Done")
    return Response(status=200, mimetype='application/json')

@app.route("/join/progress", methods=["GET"])
def data_loading_join_progress():
    # here we want to get the value of user (i.e. ?user=some-value)
    id = request.args.get('id')
    logging.info("The Joining id is --> "+str(id))

    random_number = random.randint(1, 10)
    if(random_number<7):
        return "progress"
    else:
        return "complete"

@app.route("/data-cleaning/progress", methods=["GET"])
def data_cleaning_progress():
    # here we want to get the value of user (i.e. ?user=some-value)
    id = request.args.get('id')
    logging.info("The Cleaning id is --> "+str(id))

    random_number = random.randint(1, 10)
    if(random_number<7):
        return "progress"
    else:
        return "complete"

@app.route("/data-ingesting/progress", methods=["GET"])
def data_ingesting_progress():
    id = request.args.get('id')
    logging.info("The Ingestion id is --> "+str(id))

    random_number = random.randint(1, 10)
    if(random_number<7):
        return "progress"
    else:
        return "complete"

@app.route("/data-ingesting/<jobid>", methods=["GET"])
def data_ingesting_results(jobid):
    features = {"features": ["column1", "column2", "column3"]}
    return jsonify(features)


if __name__ == "__main__":
    app.run("localhost", 5001, True)