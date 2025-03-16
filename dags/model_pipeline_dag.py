import json
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from scipy.stats import linregress

with open("dags/datasets_config.json") as f:
    config = json.load(f)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 15),
    "retries": 1,
}


def load_data(dataset_path: str, **kwargs):
    train_path = os.path.join(dataset_path, "train.parquet")
    test_path = os.path.join(dataset_path, "test.parquet")
    train = pd.read_parquet(train_path)
    test = pd.read_parquet(test_path)
    kwargs["ti"].xcom_push(key="train", value=train)
    kwargs["ti"].xcom_push(key="test", value=test)


def validate_data(**kwargs):
    ti = kwargs["ti"]
    train = ti.xcom_pull(task_ids="load_data", key="train")  # Pull train data from XCom
    test = ti.xcom_pull(task_ids="load_data", key="test")  # Pull test data from XCom
    assert not train.empty, "Train dataset is empty"
    assert not test.empty, "Test dataset is empty"
    assert "distance" in train.columns, "Train dataset missing 'distance' column"
    assert (
        "trip_duration" in train.columns
    ), "Train dataset missing 'trip_duration' column"
    assert "distance" in test.columns, "Test dataset missing 'distance' column"
    assert (
        "trip_duration" in test.columns
    ), "Test dataset missing 'trip_duration' column"
    assert (
        train["distance"].dtype == "float64"
    ), "Train dataset 'distance' column is not of type float64"
    assert (
        train["trip_duration"].dtype == "float64"
    ), "Train dataset 'trip_duration' column is not of type float64"
    assert (
        test["distance"].dtype == "float64"
    ), "Test dataset 'distance' column is not of type float64"
    assert (
        test["trip_duration"].dtype == "float64"
    ), "Test dataset 'trip_duration' column is not of type float64"


def train_model(model_path: str, **kwargs):
    ti = kwargs["ti"]
    train = ti.xcom_pull(task_ids="load_data", key="train")
    X_train = train["distance"].to_numpy()
    y_train = train["trip_duration"].to_numpy()
    model = linregress(X_train, y_train)
    joblib.dump(model, model_path)


def evaluate_model(model_path: str, **kwargs):
    ti = kwargs["ti"]
    test = ti.xcom_pull(task_ids="load_data", key="test")
    X_test = test["distance"].to_numpy()
    y_test = test["trip_duration"].to_numpy()
    model = joblib.load(model_path)
    y_pred_test = model.slope * X_test + model.intercept
    performance = np.sqrt(np.average((y_pred_test - y_test) ** 2))
    ti.xcom_push(key="performance", value=performance)


def check_deployable(**kwargs):
    ti = kwargs["ti"]
    performance = ti.xcom_pull(task_ids="evaluate_model", key="performance")
    performance_threshold = 500
    if performance <= performance_threshold:
        return "deploy"
    else:
        return "notify"


for dataset_config in config["datasets"]:
    dataset_name = dataset_config["name"]
    dataset_path = dataset_config["path"]
    model_path = dataset_config["model_path"]

    @dag(
        dag_id=f"model_deployment_dag_{dataset_name}",
        default_args=default_args,
        description=f"A DAG for model deployment with data validation and evaluation for {dataset_name}",
        schedule_interval="@once",
    )
    def model_deployment_dag():
        start = DummyOperator(task_id="start")
        load_data_task = PythonOperator(
            task_id="load_data",
            python_callable=load_data,
            op_kwargs={"dataset_path": dataset_path},
        )
        validate_data_task = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
        )
        train_model_task = PythonOperator(
            task_id="train_model",
            python_callable=train_model,
            op_kwargs={"model_path": model_path},
        )
        evaluate_model_task = PythonOperator(
            task_id="evaluate_model",
            python_callable=evaluate_model,
            op_kwargs={"model_path": model_path},
        )
        is_deployable = BranchPythonOperator(
            task_id="is_deployable",
            python_callable=check_deployable,
            provide_context=True,
        )
        deploy = DummyOperator(task_id="deploy")
        notify = DummyOperator(task_id="notify")
        end = DummyOperator(task_id="end")

        (
            start
            >> load_data_task
            >> validate_data_task
            >> train_model_task
            >> evaluate_model_task
            >> is_deployable
        )
        is_deployable >> [deploy, notify] >> end

    globals()[f"model_deployment_dag_{dataset_name}"] = model_deployment_dag()
