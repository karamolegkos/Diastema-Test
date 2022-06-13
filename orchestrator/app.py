# Import custom Libraries
from pb_check import playbook_check as pbc
from analysis_handler import analysis_thread as analysis_t
from ingestion_handler import ingestion_thread as ingestion_t

# Import Libraries
import os
from flask import Flask, request, Response, make_response
import threading

""" Environment Variables """
# Flask app Host and Port
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", 5000))

# Diastema Token Environment
DIASTEMA_KEY = os.getenv("DIASTEMA_KEY", "diastema-key")

""" Global variables """
# The name of the flask app
app = Flask(__name__)

""" Flask endpoints """
# Main analysis route
@app.route("/analysis", methods=["POST"])
def analysis():
    print("[INFO] Accepted Request.")
    playbook = request.json

    # Check for mistakes in the playbook
    print("[INFO] Checking for mistakes in Diastema Playbook.")
    pb_validity = pbc(playbook, DIASTEMA_KEY)
    print("[INFO] Playbook Check up is completed.")

    # If a problem occurs then return unauthorised
    if pb_validity[0] == False:
        return Response('{"reason": '+pb_validity[1]+'}', status=401, mimetype='application/json')

    print("[INFO] Starting a new Thread for the analisys.")
    thread = threading.Thread(target = analysis_t, args = (playbook, ))
    thread.start()
    print("[INFO] Analysis started.")

    print("[INFO] Returned Status 202.")
    return Response(status=202)

# Main ingection route
@app.route("/ingestion", methods=["POST"])
def ingestion():
    print("[INFO] Accepted Request.")
    playbook = request.json

    # There should be checkups here for the playbook, in the feature

    print("[INFO] Starting a new Thread for the ingestion.")
    thread = threading.Thread(target = ingestion_t, args = (playbook, ))
    thread.start()
    print("[INFO] ingestion started.")

    print("[INFO] Returned Status 202.")
    return Response(status=202)

""" Main """
# Main code
if __name__ == "__main__":
    app.run(HOST, PORT, True)