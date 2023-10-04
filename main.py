import requests
import json
import time
import functions_framework
from google.auth.transport.requests import Request
import google.auth


dataform_project = "cloudfunction-dataform-demo"
dataform_location = "us-central1"
dataform_repository = "test"
dataform_workspace = "test"

#variables for the status_chck
initial_wait = 10
wait_interval = 10
wait_iterations  = 100


# Don't forget to update this variable after updating model. This variable is not used if you call "/compile" endpoint
compilation_result_path = "projects/cloudfunction-dataform-demo/locations/us-central1/repositories/test/compilationResults/a6887a3c-799a-4829-a8c2-bf0d2032fa2f"


@functions_framework.http
def trigger_dataform(request):

    """
    This function compiles and triggers a dataform workflow and return status.
    """
    #Check the URL path
    if 'compile' in request.path:
        compile = True
    else:
        compile = False

    # Create auth token for the api. Important: the service account that
    # runs the cloud function must have dataform.editor role
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    credentials, project = google.auth.default(scopes=scopes)
    # If necessary, refresh the credentials
    if not credentials.valid:
        credentials.refresh(Request())
    api_auth_token = credentials.token
    # print(api_auth_token)

    api_url = "https://dataform.googleapis.com/v1beta1"
    repository_path = f"projects/{dataform_project}/locations/{dataform_location}/repositories/{dataform_repository}"
    api_compilation_url = f"{api_url}/{repository_path}/compilationResults"
    api_workflow_invocations_url = f"{api_url}/{repository_path}/workflowInvocations"
    workspace = f"{repository_path}/workspaces/{dataform_workspace}"

    headers = {
        "Authorization": f"Bearer {api_auth_token}",
        "Content-Type": "application/json",
    }


    if compile:

        # Compile the dataform workflow. This is needed otherwise the workflow invocation will fail
        print("try to compile worflow")
        payload = json.dumps({"workspace": workspace})
        response = requests.request(
            "POST", api_compilation_url, data=payload, headers=headers
        )
        if response.status_code != 200:
            raise Exception(
                f"Request to {api_compilation_url} failed with status code {response.status_code}: {response.text}"
            )
        print(response.status_code)
        print(response.json())
        compliation_name = response.json()["name"]
        print("###############################################")
        print("Compilation result name: " + compliation_name)
        print("###############################################")
    else:
        compliation_name= compilation_result_path

    # Trigger the dataform workflow
    print("try to trigger workflow")
    payload = json.dumps({"compilationResult": compliation_name})
    # Optional, you can add InvocationConfig to the payloud
    payload = json.dumps({"compilationResult": compliation_name, "invocationConfig": {"includedTags": ["tag1"]}})

    response = requests.request(
        "POST", api_workflow_invocations_url, headers=headers, data=payload
    )
    if response.status_code != 200:
        raise Exception(
            f"Request to {api_workflow_invocations_url} failed with status code {response.status_code}: {response.text}"
        )

    response_json = response.json()
    invocation_id = response_json["name"]

    print(response.status_code)
    print(response_json)

    time.sleep(initial_wait)

    print("Check status")

    # Wait until the workflow is done
    for i in range (wait_iterations):
        response = requests.request(
            "GET", api_workflow_invocations_url, headers=headers
        )
        if response.status_code != 200:
            raise Exception(
                f"Check status failed with status code {response.status_code}: {response.text}"
            )
        response_json = response.json()
        for row in response_json["workflowInvocations"]:
            if row["name"] == invocation_id:
                if row["state"] == "SUCCEEDED":
                    return "success"

        time.sleep(wait_interval)

    return "timeout"

