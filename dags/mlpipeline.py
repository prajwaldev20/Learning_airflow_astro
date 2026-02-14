from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Define task 1
def preprocess_data():
    print("Preprocessing the data..")

## Define task 2
def train_model():
    print("Training the model..")

# Define task 3
def evaluate_model():
    print("Evaluate Model..")

## Define DAG
with DAG(
    'ml_pipeline',
    start_date = datetime(2026,2,13),
    schedule = '@weekly',
    catchup = False,
    tags = ["ml"],
) as dag:
    
    # Define the task
    preprocess = PythonOperator(task_id="preprocess_task", python_callable=preprocess_data) 
    train = PythonOperator(task_id="train_task", python_callable=train_model)
    evaluate = PythonOperator(task_id = "evaluate_task", python_callable = evaluate_model)


    ## Set dependency 
    preprocess >> train >> evaluate


