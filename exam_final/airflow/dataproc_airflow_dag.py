import uuid
import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = "ru-central1-b"
YC_DP_SSH_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILy2YL1j5Jw59+FyTh/lOBo/n+M52gyWmrbu4Ix3dgHC medit3rranean@wak3uphomie"
YC_DP_SUBNET_ID = "e2lurjnl5hsgb929el1l"
YC_DP_SA_ID = "ajevtu8ntg1uurlpv8vm"
YC_BUCKET = "etl-module4-kononenko"

CLUSTER_NAME = "module4-dataproc-cluster"

with DAG(
    dag_id="module4_dataproc_etl",
    schedule=None,
    start_date=datetime.datetime(2026, 6, 1),
    max_active_runs=1,
    catchup=False,
    tags=["module4", "dataproc", "etl"],
) as dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=f"tmp-dp-{uuid.uuid4()}",
        cluster_description="Temporary cluster for PySpark ETL job",
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version="2.1",

        masternode_resource_preset="s2.small",
        masternode_disk_type="network-ssd",
        masternode_disk_size=64,

        computenode_resource_preset="s2.small",
        computenode_disk_type="network-ssd",
        computenode_disk_size=64,
        computenode_count=1,
        computenode_max_hosts_count=1,

        services=["YARN", "SPARK"],
        datanode_count=0,
    )

    run_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_job",
        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster') }}",
        main_python_file_uri=f"s3a://{YC_BUCKET}/scripts/process_applications.py",
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> run_pyspark_job >> delete_spark_cluster