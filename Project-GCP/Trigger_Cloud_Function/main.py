import logging
import time
from google.cloud import dataproc_v1 as dataproc

def start_dataproc_cluster(data, context):
    """Triggered by a change to a Cloud Storage bucket."""
    file_name = data['name']
    file_path = file_name.split('/')
    
    # Check if the file is in the desired folder
    if len(file_path) > 1 and file_path[0] == 'trigger':
        # Log the file upload info
        bucket_name = "ilaya"
        logging.info(f"New file uploaded: {file_name} in bucket: {bucket_name}")
        
        # Define your Dataproc cluster parameters
        project_id = "circular-gist-407717"
        region = "asia-south2"
        cluster_name = "ilaya-cluster"
        service_account = "circular-gist-407717@appspot.gserviceaccount.com"
        
        # Start the Dataproc cluster
        create_dataproc_cluster(project_id, region, cluster_name, service_account)
        
        return f"File {file_name} in bucket {bucket_name} triggered Dataproc cluster start and Spark job submission."

    else:
        logging.info(f"File {file_name} is not in the desired folder 'trigger1'. Ignoring.")

def create_dataproc_cluster(project_id, region, cluster_name, service_account):
    # Create a Dataproc client
    dataproc_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    # Define cluster config
    cluster_config = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "software_config": {"image_version": "2.1-debian11"},
            "gce_cluster_config": {"service_account": service_account}
        },
    }

    try:
        # Create the Dataproc cluster
        operation = dataproc_client.create_cluster(
            request={"project_id": project_id, "region": region, "cluster": cluster_config}
        )
        logging.info(f"Starting Dataproc cluster {cluster_name}.")
        
        # Wait for the cluster to reach the "RUNNING" state
        while True:
            cluster = dataproc_client.get_cluster(
                project_id=project_id, region=region, cluster_name=cluster_name
            )
            cluster_status = cluster.status.state
            if cluster_status == dataproc.ClusterStatus.State.RUNNING:
                logging.info(f"Dataproc cluster {cluster_name} is in the RUNNING state.")
                break
            elif cluster_status == dataproc.ClusterStatus.State.ERROR:
                raise Exception(f"Cluster creation failed. Cluster state: {cluster_status}")
            
            time.sleep(10)  # Wait for 10 seconds before checking again
        
        # After the cluster is running, submit your Spark job
        submit_spark_job(project_id, region, cluster_name)
        
    except Exception as e:
        logging.error(f"An error occurred while creating the cluster: {str(e)}")

def submit_spark_job(project_id, region, cluster_name):
    # Create a Dataproc client
    dataproc_client = dataproc.JobControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    
    # Specify Spark job details
    spark_jar_uri = "gs://ilaya/Jars/project-gcp_2.12-1.0.jar"
    spark_main_class = "GCP.ProjectGCP"
    spark_args = []
    
    job = {
        "placement": {"cluster_name": cluster_name},
        "spark_job": {
            "main_class": spark_main_class,
            "jar_file_uris": [spark_jar_uri],
            "args": spark_args,
        },
    }
    
    operation = dataproc_client.submit_job(project_id=project_id, region=region, job=job)
    logging.info("Submitted Spark job to Dataproc cluster.")