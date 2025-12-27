from prefect import flow, task
from etl_pipeline.bronze_layer.bronze_pipeline import BronzePipeline
from etl_pipeline.silver_layer.silver_pipeline import SilverPipeline
from etl_pipeline.load.load_pipeline import LoadPipeline, _get_project_root

@task(
        task_run_name="bronze_layer_task", 
        retries=3, 
        retry_delay_seconds=[3, 5, 10], 
        timeout_seconds=60,
        log_prints=True
)
def extract_data():
    bronze_int = BronzePipeline()

    bronze_int.run_full_extraction()
    bronze_int.generate_report()

@task(
        task_run_name="silver_layer_task",
        retries=2,
        retry_delay_seconds=[1, 3],
        timeout_seconds= 30,
        log_prints=True
)
def transform_data():
    silver_init = SilverPipeline()

    silver_init.run_full_transformation()
    silver_init.generate_report()

@task(
        task_run_name="load_layer_task",
        retries=2,
        timeout_seconds=30,
        log_prints=True
)
def load_data():
    project_root = _get_project_root()

    loader = LoadPipeline(project_root)
    loader.load_all()


@flow(flow_run_name="systock_etl")
def main():
    task_bronze = extract_data()
    task_silver = transform_data(wait_for=[task_bronze])
    load_data(wait_for=[task_silver])

if __name__ == '__main__':
    main.serve(name="daily-midnight", cron="0 0 * * 1-6")