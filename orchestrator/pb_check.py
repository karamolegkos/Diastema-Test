# Function to check if a Diastema Playbook is Valid

def playbook_check(playbook, diastema_token):
    """
    A set of rules.
    Returns True is Playbook is ok. 
    Or False if there is a problem in the playbook.
    """

    if playbook is None:
        print("[ERROR] No Diastema playbook given!")
        return (False, "No Diastema playbook given!")
    
    if not ("diastema-token" in playbook):
        print("[ERROR] No Diastema Token given!")
        return (False, "No Diastema Token given!")

    if playbook["diastema-token"] != diastema_token:
        print("[ERROR] Invalid Diastema Token!")
        return (False, "Invalid Diastema Token!")

    # More rules can be added here

    return (True, "Valid playbook")