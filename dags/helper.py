import urllib.request
import json
from constants import portinar_username , portinar_password , portinar_url

def run_portainer_container(payload : dict):
    base_url = f"{portinar_url}/api/endpoints/2/docker"
    
    jwt = auth_portainer()
    
    headers = {'Content-Type': 'application/json',
               "Authorization": f"Bearer {jwt}"}
    create_container_request = urllib.request.Request(
        url=base_url + "/containers/create",
        data = json.dumps(payload).encode('utf-8'),
        headers=headers  ,
        method="POST"
    )

    create_container_response = urllib.request.urlopen(create_container_request)
    
    create_container_response_data = json.loads(create_container_response.read().decode("utf-8"))
    container_id = create_container_response_data.get("Id")
    create_container_response_status_code = create_container_response.getcode()
    if not container_id:
            raise ValueError("Failed to retrieve container ID from response.")
    print(f"Container created successfully with ID: {container_id} (Status Code: {create_container_response_status_code})")
        
    print(f"Starting the container with ID: {container_id}...")
    start_container_endpoint_template = f"{base_url}/containers/{container_id}/start"

    start_container_endpoint = start_container_endpoint_template.format(container_id=container_id)
    start_request = urllib.request.Request(
        url=start_container_endpoint,
        headers=headers,
        method="POST"
    )
    start_response = urllib.request.urlopen(start_request)
    start_status_code = start_response.getcode()

    if start_status_code != 204:
        raise ValueError(f"Unexpected status code while starting container: {start_status_code}")
    print(f"Container with ID {container_id} started successfully.")

        
        
        
def auth_portainer():
    
    headers = {
    'Content-Type': 'application/json'
    }

    data = {
    "Username": portinar_username,
    "Password": portinar_password
    }
    
    
    url = f"{portinar_url}/api/auth"
    request = urllib.request.Request(
        url=url,
        data=json.dumps(data).encode('utf-8'),
        headers=headers,
        method="POST"
    )
    print("Create Connection ...")
    response = urllib.request.urlopen(request)
    print(f"Response Code : {response.getcode()}")
    json_response = json.loads(response.read().decode('utf-8'))
    if response.getcode() != 200 :
        raise Exception(f"Auth faild with response : {json_response}")
    
    jwt = json_response.get("jwt")
    print(f"Access Token : {jwt}")
    return jwt
    
def nessie_config():
    
    
    
    pass