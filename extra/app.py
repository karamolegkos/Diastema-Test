import os
from flask import Flask, request, Response
import requests

HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", 80))

ORCHESTRATOR_HOST = os.getenv("ORCHESTRATOR_HOST", "localhost")
ORCHESTRATOR_PORT = int(os.getenv("ORCHESTRATOR_PORT", 5000))

app = Flask(__name__)

""" Flask endpoints """
# Main analysis route
@app.route("/analysis", methods=["POST"])
def analysis():
    print("[INFO] Accepted Request.")
    playbook = request.json
    url = "http://"+ORCHESTRATOR_HOST+":"+str(ORCHESTRATOR_PORT)+"/analysis"

    print("[INFO] making request.")
    res = requests.post(url, json=playbook)
    if res.status_code == 202:
	    return "OK"
    else:
        return "NOT OK"

""" Main """
# Main code
if __name__ == "__main__":
    app.run(HOST, PORT, True)