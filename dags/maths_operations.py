"""
We'll define a DAG where tasks are as follows:

Task 1: Start with an initial number (e.g., 10)
Task 2: Add 5 to the number
Task 3: Multiply the result by 2
Task 4: Subtract 3 from the result
Task 5: Compute the square of the result

"""

from airflow import DAG
from airflow.operators.python import PythonOperators
from datetime import datetime

## Define function for each task

def start_number(**context):
    context["ti"].xcom_push(key='current_value', value = 10)



    print("The starting number is 10")


def add_five(**context):
    current_value = context["ti"].xcom_pull(key = 'current_value', task_ids ='start_task')
    new_value = current_value + 5
    context["ti"].xcom_push(key = 'current_value', value = new_value)
    print(f"Add 5: {current_value} +5 = {new_value}")

def multiply_by_two(**context):
    current_value = context["ti"].xcom_pull(key='current_value', task_ids = 'add_five_task')
    new_value = current_value*2
    context["ti"].xcom_push(key='current_value', value= new_value)
    print(f"Multiply by 2: {current_value} *2 = {new_value}")

def subtract_by_three(**context):
    current_value = context["ti"].xcom_pull(key='current_value', task_ids = 'multiply_by_two_task')
    new_value = current_value - 3
    context["ti"].xcom_push(key='current_value', value = new_value)
    print(f"Subtract 3: {current_value} - 3 = {new_value}")
    

def square_number(**context):
    current_value = context["ti"].xcom_pull(key='current_value', task_ids = 'subtract_by_three_task')
    new_value = current_value **2
    
    print(f"Square the result: {current_value}^2 = {new_value}")
