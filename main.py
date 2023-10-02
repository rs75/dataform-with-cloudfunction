import requests
import json
import functions_framework
from google.auth.transport.requests import Request
import google.auth

dataform_project = "cloudfunction-dataform-demo"
dataform_location = "us-central1"
dataform_repository = "test"
dataform_workspace = "test"


@functions_framework.http
def trigger_dataform(request):
    """
    This function compiles and triggers a dataform workflow and return status.
    """

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

    # Trigger the dataform workflow
    print("try to trigger workflow")

    payload = json.dumps({"compilationResult": compliation_name})
    # Optional, you can add InvocationConfig to the payloud
    # https://cloud.google.com/dataform/reference/rest/v1beta1/InvocationConfig
    # e.g. payload = json.dumps({"compilationResult": compliation_name, "InvocationConfig" : { "includedTags": ["tag1"]}})

    response = requests.request(
        "POST", api_workflow_invocations_url, headers=headers, data=payload
    )
    if response.status_code != 200:
        raise Exception(
            f"Request to {api_workflow_invocations_url} failed with status code {response.status_code}: {response.text}"
        )
    print(response.status_code)
    response_json = response.json()
    print(response_json)

    return "Done - Dataform state is: " + response_json["state"]


# uncomment for local testing
# if __name__ == "__main__":
#    trigger_dataform(None)
