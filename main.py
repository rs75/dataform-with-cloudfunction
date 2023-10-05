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

# call "/compile" to create compilation result or copy from dataform console
compilation_result_path = "projects/cloudfunction-dataform-demo/locations/us-central1/repositories/test/compilationResults/5f0204dc-c05f-43e1-b65d-c088c40ad403"
release_config_name = "v1" # this must exist in dataform console

tags = ["tag1"]

password = "test"

@functions_framework.http
def trigger_dataform(request):
    """
    This function compiles and triggers a dataform workflow and return status.
    """
    try:
        # #check password
        query_password = request.args.get('password')

        if not query_password:
            return 'Wrong password'
        if query_password != password:
            return 'Wrong password'

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
        #workspace = f"{repository_path}/workspaces/{dataform_workspace}"
        release_config = f"{repository_path}/releaseConfigs/{release_config_name}"

        headers = {
            "Authorization": f"Bearer {api_auth_token}",
            "Content-Type": "application/json",
        }

        if compile:

            # Compile the dataform workflow. This is needed otherwise the workflow invocation will fail
            print("try to compile worflow")
            payload = json.dumps({"releaseConfig": release_config})
            response = requests.request(
                "POST", api_compilation_url, data=payload, headers=headers
            )
            if response.status_code != 200:
                return f"Request to {api_compilation_url} failed with status code {response.status_code}: {response.text}"

            print(response.status_code)
            print(response.json())
            compliation_name = response.json()["name"]
            print("###############################################")
            print("release config full name: " + compliation_name)
            print("###############################################")
        else:
            compliation_name= compilation_result_path

        # Trigger the dataform workflow
        print("try to trigger workflow")
        # Optional, you can add InvocationConfig to the payloud
        payload = json.dumps({"compilationResult": compliation_name, "invocationConfig": {"includedTags": tags}})

        response = requests.request(
            "POST", api_workflow_invocations_url, headers=headers, data=payload
        )
        if response.status_code != 200:
            return  f"Request to {api_workflow_invocations_url} failed with status code {response.status_code}: {response.text}"

        response_json = response.json()
        invocation_id = response_json["name"].split("/")[-1]

        print(response.status_code)
        print(response_json)

        time.sleep(initial_wait)

        print("Check status")

        # Wait until the workflow is done
        for i in range (wait_iterations):
            response = requests.request(
                "GET", f'{api_workflow_invocations_url}/{invocation_id}', headers=headers
            )
            if response.status_code != 200:
                return "Check status failed with status code {response.status_code}: {response.text}"

            response_json = response.json()
            if response_json ["state"] == "SUCCEEDED":
                if compile:
                    return f'success -  Release config created: {compliation_name}'
                return "success"
            if response_json ["state"] == "FAILED":
                return "failed"

            time.sleep(wait_interval)

        return "timeout"
    except Exception as e:
        return f"Error: {e}"




#if __name__ == "__main__":
#    print(trigger_dataform(None))