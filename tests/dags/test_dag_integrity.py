
from airflow.executors.debug_executor import DebugExecutor
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.dates import days_ago
import time
from datetime import timedelta

# validate numbers of tasks in DAG
def test_task_count():
    """Check task count of Recipes_pipeline dag"""
    dag_id = "Recipes_pipeline"
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag(dag_id)
    assert len(dag.tasks) == 4

# validate tasks' name in DAG
def test_contain_tasks():
    """Check task contains in Recipes_pipeline dag"""
    dag_id = "Recipes_pipeline"
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag(dag_id)
    tasks = dag.tasks
    task_ids = list(map(lambda task: task.task_id, tasks))
    assert set(task_ids).intersection(["get_recipes_from_api", "get_top10_recipes", "upload_recipes_to_s3", "upload_top10_recipes_to_s3"])

# validate running DAG
def test_run_dag():
    """Check Recipes_pipeline dag should run successfully"""
    dag_id = "Recipes_pipeline"
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag(dag_id)

    start_date = dag.start_date
    end_date = dag.start_date
    exe_date = start_date + timedelta(days=1)

    # execute DAG
    dag.clear()
    dag.run(executor=DebugExecutor(), start_date=start_date, end_date=end_date)

    dagruns = DagRun.find(dag_id=dag.dag_id, execution_date=exe_date)
    assert len(dagruns) == 1

    is_running = True
    while is_running:
        dagruns = DagRun.find(dag_id=dag.dag_id, execution_date=exe_date)
        is_running = True if dagruns[0].state == "running" else False
        time.sleep(2)
    # Validate DAG run was successful
    assert dagruns[0].state == "success"

# test the validity of all DAGs. 
# for instance, all tasks have required arguments, no cycles in the DAGs, etc.
def test_validation_all_DAGs():
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors  # check import errors
