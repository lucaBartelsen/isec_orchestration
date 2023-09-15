"""
This script loads configuration settings from a config file and uses them to initiate a machine
group scan, deploy patches, and shut down machines.
"""

import socket
import time
import requests
import configparser
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import json
import logging
import os
import datetime


# These lines are initializing global variables to be used throughout the script. They are all set to
# `None` initially and will be assigned values later in the `load_config()` function.
# global variables
server = None
first_ring_id = None
second_ring_id = None
auth = None
verify = None
run_as_credentials = None
scan_template = None
machine_group_server = None
machine_group_database = None
deployment_template = None
vcenter_server = None
vcenter_username = None
vcenter_password = None


def load_config():
    """
    This function loads configuration settings from a config file and sets them as global variables.
    """
    # define variables as global
    global server, first_ring_id, second_ring_id, auth, verify, run_as_credentials, scan_template, machine_group_database, machine_group_server, deployment_template, vcenter_password, vcenter_server, vcenter_username
    # Create a ConfigParser object
    config = configparser.ConfigParser()
    # Read the config file
    config.read('config.ini')

    # Get the server variable from the [Server] section
    server = config.get('Server', 'server')
    vcenter_username = config.get('Server', 'vcenter_username')
    vcenter_password = config.get('Server', 'vcenter_password')
    vcenter_username = config.get('Server', 'vcenter_username')

    auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    verify = config.get('Server', 'path_to_cert')
    
    run_as_credentials = config.get('Configuration', 'run_as_credentials')
    scan_template = config.get('Configuration', 'scan_template')
    deployment_template = config.get('Configuration', 'deployment_template')
    machine_group_server = config.get('Configuration', 'machine_group_server')
    machine_group_database = config.get('Configuration', 'machine_group_database')

    logpath = config.get('Logging', 'logpath')
    loglevel = config.get('Logging', 'loglevel')

    init_logging(logpath, loglevel)

    logging.debug(f"Server FQDN: {server}")
    logging.info("Config loaded")

    
def init_logging(logpath, loglevel):
    """
    This function initializes logging with a specified log path and log level.
    
    :param logpath: The path where the log file will be saved
    :param loglevel: The desired logging level, which can be one of the following strings: "DEBUG",
    "INFO", "WARNING", "ERROR", or "CRITICAL"
    """
    if loglevel == "DEBUG":
        loglevel = logging.DEBUG
    if loglevel == "INFO":
        loglevel = logging.INFO
    if loglevel == "WARNING":
        loglevel = logging.WARNING
    if loglevel == "ERROR":
        loglevel = logging.ERROR
    if loglevel == "CRITICAL":
        loglevel = logging.CRITICAL

    if not os.path.exists(logpath):
        os.makedirs(logpath, exist_ok=True)
    current_time = datetime.datetime.now()
    logfile = os.path.join(logpath, f'orchestration-{current_time.year}-{current_time.month}-{current_time.day}-{current_time.hour}-{current_time.minute}-{current_time.second}.log')
    logfile = os.path.expandvars(logfile)

    logging.basicConfig(filename=logfile, level=loglevel, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.info("Logging started")

def log(response):
    """
    This function logs information about a HTTP request and its response.
    
    :param response: The response object returned by a HTTP request. It contains information such as the
    status code, headers, and response body
    """
    logging.info(f"Requested URL: {response.request.url}")
    logging.info(f"Requested methode: {response.request.method}")
    logging.info(f"Request Body: {response.request.body}")
    logging.info(f"Final response code: {response.status_code}")
    logging.debug(f"Response: {response.text}")


def create_session():
    """
    This function creates a session by sending a POST request to a server with authentication and
    logging information.
    """
    url = f"{server}/st/console/api/v1.0/sessioncredentials"

    data = {
    "clearText": "Pa$$w0rd",
    "protectionMode": "None"
    }

    payload = json.dumps(data)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "POST", url, auth=auth, headers=headers, data=payload, verify=verify)
    
    logging.info("Creating a Session")
    log(response)

def delete_session():
    """
    This function sends a DELETE request to a specified URL to remove a session using provided
    authentication credentials and logs the response.
    """
    url = f"{server}/st/console/api/v1.0/sessioncredentials"

    response = requests.request(
        "DELETE", url, auth=auth, verify=verify)

    logging.info("Removing the Session")
    log(response)


def get_id_by_name(api_endpoint, name):
    """
    This function retrieves the ID of an item by its name from a specified API endpoint.
    
    :param api_endpoint: The API endpoint is a string that specifies the endpoint of the API that we
    want to access. It could be something like "users", "products", "orders", etc
    :param name: The name of the item you want to get the ID for
    :return: the id of an item with a specific name from a JSON response obtained from a specified API
    endpoint.
    """
    if api_endpoint == 'machinegroups':
        url = f"{server}/st/console/api/v1.0/{api_endpoint}/?count=1000"
    elif api_endpoint == 'credentials':
        url = f"{server}/st/console/api/v1.0/{api_endpoint}/?name={name}"
    else:
        url = f"{server}/st/console/api/v1.0/{api_endpoint}"

    response = requests.get(url, auth=auth, verify=verify)

    logging.info(f"Getting the {api_endpoint} id for the {name} {api_endpoint}")
    log(response)

    py_obj = response.json()
    for item in py_obj["value"]:
        if item["name"] == name:
            return item["id"]
            
def get_run_as_credentials_id(run_as_credentials_name):
    return get_id_by_name("credentials", run_as_credentials_name)
    
def get_scan_template_id(scan_template_name):
    return get_id_by_name("patch/scanTemplates", scan_template_name)

def get_deployment_template_id(deployment_template_name):
    return get_id_by_name("patch/deploytemplates", deployment_template_name)

def get_machine_group_id(machine_group_name):
    return get_id_by_name("machinegroups", machine_group_name)

def scan_machine_group(machine_group_id, scan_template_id, credential_id):
    """
    The function initiates a machine group scan using a specified scan template and credential ID.
    
    :param machine_group_id: The ID of the machine group that needs to be scanned
    :param scan_template_id: The ID of the scan template that will be used for the scan
    :param credential_id: The ID of the credential that will be used to run the scan on the machines in
    the specified machine group
    :return: the ID of the scan that was initiated on the specified machine group using the specified
    scan template and credential.
    """
    data = {
        "machineGroupIds": [machine_group_id],
        "name": "Machine Group Scan iniated by Rest API",
        "templateId": scan_template_id,
        "runAsCredentialId": credential_id
    }

    url = f"{server}/st/console/api/v1.0/patch/scans"

    payload = json.dumps(data)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "POST", url, auth=auth, headers=headers, data=payload, verify=verify)
    
    logging.info(f"Starting the Machine Group Scan on Machine Group ID {machine_group_id}")
    log(response)
    return response.json()["id"]

def operation_status(id):
    """
    This function retrieves the status of a process operation with a given ID from a server API.
    
    :param id: The id parameter is the unique identifier of the process for which we want to retrieve
    the operation status
    :return: the response object obtained from making a GET request to a specific URL. The response
    object contains information about the status of an operation with a given ID.
    """
    url = f"{server}/st/console/api/v1.0/operations/{id}"

    response = requests.get(url, auth=auth, verify=verify)

    logging.info(f"Getting the operation status for the process with the id {id}")
    log(response)

    return response

def patch_deployment(machine_group_server_scan_id, deployment_template_id):
    """
    This function deploys a patch to a machine group server scan using a deployment template ID.
    
    :param machine_group_server_scan_id: The ID of the server scan that the patch deployment will be
    based on
    :param deployment_template_id: The ID of the deployment template that will be used for the patch
    deployment
    :return: a UUID (Universally Unique Identifier) which is extracted from the "Location" header of the
    HTTP response.
    """
    data = {
        "scanId": machine_group_server_scan_id,
        "templateId": deployment_template_id
    }

    url = f"{server}/st/console/api/v1.0/patch/deployments"

    payload = json.dumps(data)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "POST", url, auth=auth, headers=headers, data=payload, verify=verify)
    
    logging.info(f"Starting the deployment for scan iud {machine_group_server_scan_id}")
    log(response)
    uuid = response.headers["Location"].split("/")[-1] 
    return uuid

def start_deployment(machine_group_server_scan_id, deployment_template_id):
    """
    This function waits for a server scan to succeed before starting a patch deployment.
    
    :param machine_group_server_scan_id: This parameter is likely an ID or identifier for a machine
    group server scan. It is used in the function to check the status of the scan and wait until it
    reaches a desired status before continuing with the patch deployment
    :param deployment_template_id: The ID of the deployment template that will be used for the patch
    deployment
    :return: the result of the `patch_deployment` function with the `machine_group_server_scan_id` and
    `deployment_template_id` as arguments.
    """
    logging.info(f"Waiting for the scan to succeed and beeing able to start the patch deployment")
    
    desired_status = "Succeeded"

    while True:
        response = operation_status(machine_group_server_scan_id)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == desired_status:
                logging.info(f"Status switched to succeeded")
                break  # Exit the loop if the desired status is found
        time.sleep(30)
        logging.info(f"Waiting 30 seconds until continuing to check if the operation finished")
    
    return patch_deployment(machine_group_server_scan_id, deployment_template_id)

def get_patch_deployment_machines(id):
    """
    This function retrieves the machines associated with a patch deployment using the deployment ID.
    
    :param id: The ID of the patch deployment for which we want to retrieve the machines
    :return: a list of machine addresses for a patch deployment with the given ID.
    """

    while True: 
        status = operation_status(id).json()
        if status.get("operation") == "PatchDeployment":
            break
        time.sleep(5)

    url = f"{server}/st/console/api/v1.0/patch/deployments/{id}/machines"

    response = requests.get(url, auth=auth, verify=verify)

    logging.info(f"Getting the machines for the deployment with the id {id}")
    log(response)
    py_obj = response.json()
    ret = []
    for item in py_obj['value']:
        obj = {"machine_name": item["name"], "ip_address": item["address"]}
        ret.append(obj)
    return ret

def shutdown(ip, reboot):
    """
    The function performs a shutdown or reboot event on a specified IP address.
    
    :param ip: The IP address of the computer that needs to be shut down or rebooted
    :param reboot: A boolean value indicating whether the system should be rebooted (True) or shut down
    (False)
    :return: The result of the `os.system()` command is being returned.
    """
    if reboot:
        command = f"shutdown /r /t 0 /m \\\\{ip}"
    else:
        command = f"shutdown /s /t 0 /m \\\\{ip}"
    result = os.system(command)

    logging.info(f"Performing shutdown event on IP {ip} with reboot = {reboot}")

    return result

def wait_for_shutdown(deployment_server_machines, deployment_id, reboot_behaivior=False):
    """
    This function waits for a patch deployment to finish and then shuts down the deployment server
    machines.
    
    :param deployment_id: The ID of the deployment that is being monitored for completion
    :param reboot_behaivior: A boolean parameter that determines whether the machines should be rebooted
    after the patch deployment is complete. If set to True, the machines will be rebooted. If set to
    False, the machines will not be rebooted, defaults to False (optional)
    """
    desired_status = "Succeeded"

    while True:
        response = operation_status(deployment_id)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == desired_status:
                logging.info(f"Status switched to succeeded")
                break
        time.sleep(30)
        logging.info(f"Waiting 30 seconds until continuing to check if the operation finished")
    
    for machine in deployment_server_machines:
        shutdown(machine["ip_address"], reboot_behaivior)

def check_sql_server(ip):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, 1434))
        s.close()
        return True
    except:
        return False
    
def start_server(server_machines, database_machines):
    """
    The function starts virtual machines on a vCenter server and checks if SQL servers are running on a
    list of database machines.
    
    :param server_machines: A list of dictionaries containing information about the server machines to
    be started. Each dictionary contains the machine name and other relevant details
    :param database_machines: A list of dictionaries containing information about the database machines,
    including their IP addresses
    """
    login_url = f'{vcenter_server}/rest/com/vmware/cis/session'
    login_data = {'username': vcenter_username, 'password': vcenter_password}
    headers = {'Content-type': 'application/json'}
    response = requests.post(login_url, data=json.dumps(login_data), headers=headers, verify=False)
    session_id = response.json()['value']
    
    # The above code is implementing an infinite loop that iterates over a list of database machines.
    # For each machine, it checks if the SQL server is running by calling the `check_sql_server()`
    # function. If the server is running, it increments the `count` variable. The loop continues until
    # all the machines in the list have been checked and the `count` variable is equal to the length
    # of the list. The loop sleeps for 15 seconds between iterations.
    while True:
        for server in database_machines:
            count = 0
            if check_sql_server(server["ip_address"]):
                count += 1
        if count == database_machines.len():
            break
        time.sleep(15)

    # The above code is using the VMware vSphere REST API to start virtual machines on a vCenter
    # server. It loops through a list of server machines, gets the ID of the virtual machine
    # associated with each server machine, and then sends a POST request to start the virtual machine.
    for item in server_machines:
        vm_url = f'{vcenter_server}/rest/vcenter/vm'
        vm_data = {'filter.names': item["machine_name"]}
        headers = {'Content-type': 'application/json', 'vmware-api-session-id': session_id}
        response = requests.get(vm_url, params=vm_data, headers=headers)
        vm_id = response.json()['value'][0]['vm']
        # Start the virtual machine
        power_url = f'{vcenter_server}/rest/vcenter/vm/{vm_id}/power/start'
        power_data = {'spec': {}}
        headers = {'Content-type': 'application/json', 'vmware-api-session-id': session_id}
        response = requests.post(power_url, data=json.dumps(power_data), headers=headers)


if __name__ == '__main__':
    load_config()
    #create_session()
    credential_id = get_run_as_credentials_id(run_as_credentials)
    scan_template_id = get_scan_template_id(scan_template)
    machine_group_server_id = get_machine_group_id(machine_group_server)
    machine_group_database_id = get_machine_group_id(machine_group_database)
    deployment_template_id = get_deployment_template_id(deployment_template)
    machine_group_server_scan_id = scan_machine_group(machine_group_server_id, scan_template_id, credential_id)
    deployment_server_id = start_deployment(machine_group_server_scan_id, deployment_template_id)
    deployment_server_machines = get_patch_deployment_machines(deployment_server_id)
    wait_for_shutdown(deployment_server_machines, deployment_server_id)
    machine_group_database_scan_id = scan_machine_group(machine_group_database_id, scan_template_id, credential_id)
    deployment_database_id = start_deployment(machine_group_database_scan_id, deployment_template_id)
    deployment_database_machines = get_patch_deployment_machines(deployment_database_id)
    wait_for_shutdown(deployment_database_machines, deployment_database_id, True)