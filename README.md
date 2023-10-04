# Trigger Dataform workflow with Google Cloud Function

This is a simple example of how to trigger a Dataform workflow with a Google Cloud Function. The cloud function is triggered by a HTTP request. The cloud function then triggers a Dataform workflow by calling the Dataform API.



The function waits for the workflow to complete by iteratively checking its status. On completion, it will either return "success" or "timeout" based on the outcome. 
By default the dataform workflow is not compiled. If you want to complile, add "/compile" to the end of the url. If you do this the path to the compiled model is logged in the cloud function console. You can use this value to update the "compiled_model_path" variable in main.py. 


For setup, execute the commands below in the given order to deploy the cloud function. Ensure you update the variables at the beginning of main.py

#### Select project you want to deploy cloud function to

```
gcloud config set project <project-name>
```

#### Deploy cloud function

```
gcloud functions deploy trigger-dataform-function --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=trigger_dataform --trigger-http --allow-unauthenticated

```

It might ask you to enable apis, just say yes.
Consider removing "--allow-unauthenticated" for enhanced security.



#### Give service account of cloud function permissions for dataform 
Easiest way is to give role "roles/dataform.editor"
```
gcloud projects add-iam-policy-binding <project-name> --member=serviceAccount:<service-account-email> --role=roles/dataform.editor
```

The service account is automatically created upon deploying the first cloud function. You can find the name of the service account under the "Details" tab in the cloud function console in GCP.


