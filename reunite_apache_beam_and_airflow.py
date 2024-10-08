from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import sendgrid
from sendgrid.helpers.mail import Mail
import os
from datetime import datetime
import vertexai
from vertexai.generative_models import GenerativeModel, SafetySetting

# Define the DAG
dag = DAG(
    'reunite_apache_beam_and_airflow',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    },
    schedule_interval=None,
)

# Function to check for new files in the uploads bucket
def check_for_new_files(**kwargs):
    client = storage.Client()
    bucket_name = 'beam-summit-2024-uploads'
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs())  # List all blobs in the bucket
    
    if len(blobs) >= 5:
        # If there are 5 or more files, proceed with the DAG
        file_names = [blob.name for blob in blobs]
        kwargs['ti'].xcom_push(key='file_names', value=file_names)
        return 'move_files_to_initiated_runs'
    else:
        # If there are fewer than 5 files, branch to print message and stop
        return 'insufficient_files_task'

check_files_task = BranchPythonOperator(
    task_id='check_files_task',
    python_callable=check_for_new_files,
    provide_context=True,
    dag=dag,
)

# Task to print insufficient files message
insufficient_files_task = PythonOperator(
    task_id='insufficient_files_task',
    python_callable=lambda: print("Insufficient number of files. DAG execution stopped."),
    dag=dag,
)

# DummyOperator to end the branch when files are insufficient
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Function to move files from the uploads bucket to initiated-runs
def move_files_to_initiated_runs(**kwargs):
    client = storage.Client()
    source_bucket_name = 'beam-summit-2024-uploads'
    destination_bucket_name = 'beam-summit-2024'
    file_names = kwargs['ti'].xcom_pull(task_ids='check_files_task', key='file_names')
    
    if file_names:
        source_bucket = client.get_bucket(source_bucket_name)
        destination_bucket = client.get_bucket(destination_bucket_name)
        for file_name in file_names:
            blob = source_bucket.blob(file_name)
            new_name = f'initiated-runs/{file_name.split("/")[-1]}'  # Ensure the file is placed under initiated-runs/
            blob_copy = source_bucket.copy_blob(blob, destination_bucket, new_name)
            print(f'Copied {file_name} to {new_name}')
            blob.delete()  # Optionally delete the file from the uploads bucket after copying
            print(f'Deleted {file_name} from source bucket')
    else:
        print('No files found to move.')

move_files_task = PythonOperator(
    task_id='move_files_to_initiated_runs',
    python_callable=move_files_to_initiated_runs,
    provide_context=True,
    dag=dag,
)

# Task 2: Run the Beam pipeline on Dataflow
beam_task = DataflowCreatePythonJobOperator(
    task_id='run_beam_pipeline',
    py_file='gs://beam-summit-2024/beam_summit_attendee_upload.py',
    py_options=[],
    job_name=f'beam_pipeline_run_by_airflow_at_{datetime.now().strftime("%Y%m%d%H%M%S")}',
    dataflow_default_options={
        'project': 'your-gcp-project',
        'region': 'us-central1',
        'stagingLocation': 'gs://beam-summit-2024/staging',
        'tempLocation': 'gs://beam-summit-2024/temp'
    },
    location='us-central1',
    dag=dag,
)

# Task 3: Fetch records to send emails to, including location
def query_bigquery():
    query = """
    SELECT name, email, location
    FROM `beam-summit-2024-airflow.beam_2024_attendees.registrations`
    WHERE (is_email_sent IS NULL OR is_email_sent = FALSE) 
    AND (email <> "")
    """
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    return hook.get_pandas_df(query)

query_bigquery_task = PythonOperator(
    task_id='query_bigquery',
    python_callable=query_bigquery,
    dag=dag,
)

# Function to describe the location using Vertex AI
def describe_this_location(location):
    vertexai.init(project="beam-summit-2024-airflow", location="us-central1")
    model = GenerativeModel("gemini-1.5-flash-001")
    response = model.generate_content(
        f"Tell me some fun facts about {location} in 150 words",
        generation_config={
            "max_output_tokens": 8192,
            "temperature": 1,
            "top_p": 0.95,
        },
        safety_settings=[
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=SafetySetting.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
            ),
        ],
        stream=False
    )
    return response.text.strip()

# Task 4: Send email to the identified users
def send_email(**context):
    sg = sendgrid.SendGridAPIClient(api_key=os.environ['SENDGRID_API_KEY'])
    results = context['task_instance'].xcom_pull(task_ids='query_bigquery')
    
    for _, row in results.iterrows():
        name = row['name']
        email = row['email']
        location = row['location']
        
        # Generate fun facts about the location
        location_funfact = describe_this_location(location)
        
        # Convert the fun facts into bullet points
        bullet_points = ''.join([f'<li>{sentence.strip()}</li>' for sentence in location_funfact.split('.') if sentence.strip()])

        # Create the email content in HTML format
        subject = 'Welcome to BEAM Summit 2024 Demo by Sadeeq Akintola'
        content = f"""
        <html>
        <body>
            <p>Dear {name},</p>
            
            <p>Thank you for attending my BEAM Summit 2024 session <a href="https://beamsummit.org/sessions/2024/reuniting-the-two-distant-cousins-calling-a-beam-pipeline-from-an-airflow-job/">https://beamsummit.org/sessions/2024/reuniting-the-two-distant-cousins-calling-a-beam-pipeline-from-an-airflow-job/</a>, and testing out the demo! It means a lot to me!! Here is the link to the source code: <a href="https://github.com/SadeeqAkintola/beam-summit-2024-airflow">https://github.com/SadeeqAkintola/beam-summit-2024-airflow</a>. Fork it, Star it, and Share it, please.</p>

            <p>Please be informed that your CSV file containing <strong>{email}</strong> has been successfully uploaded.</p>

            <p><em>By the way, here are some fun facts about your {location} (generated, with love, by Google's powerful gemini-1.5-flash model):</em></p>
            <ul>{bullet_points}</ul>
            
            <p>Sincerely yours in Data Engineering,</p>
            <p><strong>Sadeeq</strong></p>
            <p>Follow on X for more: <a href="https://x.com/SadeeqAkintola">https://x.com/SadeeqAkintola</a></p>
        </body>
        </html>
        """
        
        # Send the email
        mail = Mail(from_email='datatalkswithsadeeq@gmail.com', to_emails=email, subject=subject, html_content=content)
        response = sg.send(mail)
        
        print(f'Sent email to: {email} | Status Code: {response.status_code}')

send_email_task = PythonOperator(
    task_id='send_email',
    provide_context=True,
    python_callable=send_email,
    dag=dag,
)

# Task 5: Update the is_email_sent flag in BigQuery
def update_bigquery():
    update_query = """
    UPDATE `beam-summit-2024-airflow.beam_2024_attendees.registrations`
    SET is_email_sent = TRUE
    WHERE (is_email_sent IS NULL OR is_email_sent = FALSE) 
    AND (email <> "")
    """
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    hook.run_query(update_query)

update_bigquery_task = PythonOperator(
    task_id='update_bigquery',
    python_callable=update_bigquery,
    dag=dag,
)

# Task 6: Move processed files to completed-runs
def move_files_to_completed(**kwargs):
    client = storage.Client()
    bucket_name = 'beam-summit-2024'
    blobs = client.list_blobs(bucket_name, prefix='initiated-runs/')
    for blob in blobs:
        new_name = blob.name.replace('initiated-runs/', 'completed-runs/')
        bucket = client.get_bucket(bucket_name)
        new_blob = bucket.rename_blob(blob, new_name)
        print(f'Moved {blob.name} to {new_name}')

move_files_to_completed_task = PythonOperator(
    task_id='move_files_to_completed',
    python_callable=move_files_to_completed,
    dag=dag,
)

# Setting up the task dependencies for the entire DAG
check_files_task >> insufficient_files_task >> end_task
check_files_task >> move_files_task >> beam_task >> query_bigquery_task >> send_email_task >> update_bigquery_task >> move_files_to_completed_task
