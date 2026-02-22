"""
Apache Airflow TaskFlow API

The TaskFlow API is a modern and cleaner way to define workflows (DAGs) in Airflow.
It allows developers to create tasks using Python decorators such as @dag and @task,
instead of manually defining operators like PythonOperator.

Key Features:
- Uses decorators (@dag, @task) for cleaner syntax
- Automatically handles task creation
- Supports automatic data passing between tasks using XCom
- Makes DAG code more readable and Pythonic

Core Components:
1. @dag:
   - Defines the workflow.
   - Specifies scheduling, start_date, catchup behavior, and tags.

2. @task:
   - Converts a Python function into an Airflow task.
   - Automatically manages task execution and XCom data sharing.

Advantages over traditional Operators:
- Less boilerplate code
- Easier task dependency definition
- Built-in automatic XCom handling
- Better suited for ML and data pipelines

TaskFlow API is recommended for modern Airflow DAG development.
"""


from airflow import DAG
from airflow.decorators import task
from datetime import datetime


## Define the DAG

with DAG(
    dag_id = 'math_sequence_dag_with_taskflow',
    start_date = datetime(2026,1,1),
    schedule = '@once',
    catchup = False,

) as dag:
    
    # Task 1: start with the initial number
    @task 
    def start_number():
        initial_value = 10
        print(f"Starting Number: {initial_value}")
        return initial_value
    
    # Task 2: Add 5 to the number
    @task
    def add_five(number):
        new_value = number + 5
        print(f"Add 5: {number} + 5 = {new_value}")
        return new_value
    
    # Task 3: Multiply by 2
    @task
    def multiply_by_two(number):
        new_value = number * 2
        print(f"Subtract 3: {number} -3 = {new_value}")
        return new_value
    
    # Task 4: Subtract by 3
    @task
    def subtract_three(number):
        new_value = number - 3
        print(f"Subtract 3: {number} - 3 = {new_value}")
        return new_value
    
    # Task 5: Square the number
    @task
    def square_number(number):
        new_value = number **2
        print(f"Sqaure the result: {number}^2 = {new_value}")
        return new_value
    
    ## Set task dependencies
    start_value = start_number()
    added_value = add_five(start_value)
    multiplied_value = multiply_by_two(added_value)
    subtracted_value = subtract_three(multiplied_value)
    square_value = square_number(subtracted_value)

