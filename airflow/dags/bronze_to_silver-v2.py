from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


BRONZE_TO_SILVER_YAML = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: bronze-to-silver-{{ ts_nodash | lower }}
  namespace: data-platform

spec:
  type: Python
  mode: cluster

  image: spark-bronze-to-silver:1.0
  imagePullPolicy: Never
  sparkVersion: "4.0.4"

  mainApplicationFile: local:///opt/spark/jobs/bronze_to_silver.py

  restartPolicy:
    type: Never

  sparkConf:
    spark.hadoop.fs.s3a.endpoint: "http://minio.minio.svc.cluster.local:9000"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.hadoop.fs.s3a.endpoint.region: "us-east-1"
    spark.hadoop.fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark

    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_ACCESS_KEY_ID

      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_SECRET_ACCESS_KEY

  executor:
    instances: 2
    cores: 1
    memory: "1g"

    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_ACCESS_KEY_ID

      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_SECRET_ACCESS_KEY
"""


SILVER_TO_GOLD_YAML = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: silver-to-gold-{{ ts_nodash | lower }}
  namespace: data-platform

spec:
  type: Python
  mode: cluster

  image: spark-silver-to-gold:1.0
  imagePullPolicy: Never
  sparkVersion: "4.0.4"

  mainApplicationFile: local:///opt/spark/jobs/silver_to_gold.py

  restartPolicy:
    type: Never

  sparkConf:
    spark.hadoop.fs.s3a.endpoint: "http://minio.minio.svc.cluster.local:9000"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.hadoop.fs.s3a.endpoint.region: "us-east-1"
    spark.hadoop.fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark

    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_ACCESS_KEY_ID

      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_SECRET_ACCESS_KEY

  executor:
    instances: 2
    cores: 1
    memory: "1g"

    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_ACCESS_KEY_ID

      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_SECRET_ACCESS_KEY
"""


GOLD_TO_POSTGRES_YAML = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: gold-to-postgres-{{ ts_nodash | lower }}
  namespace: data-platform

spec:
  type: Python
  mode: cluster

  image: spark-gold-to-postgres:1.0
  imagePullPolicy: Never
  sparkVersion: "4.0.4"

  mainApplicationFile: local:///opt/spark/jobs/gold_to_postgres.py

  restartPolicy:
    type: Never

  sparkConf:
    spark.hadoop.fs.s3a.endpoint: "http://minio.minio.svc.cluster.local:9000"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.hadoop.fs.s3a.endpoint.region: "us-east-1"
    spark.hadoop.fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark

    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_ACCESS_KEY_ID

      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_SECRET_ACCESS_KEY

      - name: POSTGRES_USER
        valueFrom:
          secretKeyRef:
            name: postgres-data-credentials
            key: POSTGRES_USER

      - name: POSTGRES_PASSWORD
        valueFrom:
          secretKeyRef:
            name: postgres-data-credentials
            key: POSTGRES_PASSWORD

  executor:
    instances: 1
    cores: 1
    memory: "1g"

    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_ACCESS_KEY_ID

      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-spark-credentials
            key: AWS_SECRET_ACCESS_KEY

      - name: POSTGRES_USER
        valueFrom:
          secretKeyRef:
            name: postgres-data-credentials
            key: POSTGRES_USER

      - name: POSTGRES_PASSWORD
        valueFrom:
          secretKeyRef:
            name: postgres-data-credentials
            key: POSTGRES_PASSWORD
"""


with DAG(
    dag_id="bronze_silver_gold",
    start_date=datetime(2026, 9, 27),
    schedule=None,
    catchup=False,
    tags=[
        "spark",
        "minio",
        "postgres",
        "bronze",
        "silver",
        "gold",
    ],
) as dag:

    # ---------------------------------------------------------
    # Bronze -> Silver
    # ---------------------------------------------------------

    submit_bronze_to_silver = KubernetesPodOperator(
        task_id="bronze_to_silver",
        name="submit-bronze-to-silver",
        namespace="data-platform",
        image="bitnami/kubectl:latest",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"""
cat <<'EOF' | kubectl apply -f -
{BRONZE_TO_SILVER_YAML}
EOF

APP_NAME="bronze-to-silver-{{{{ ts_nodash | lower }}}}"

echo "Waiting for $APP_NAME to finish..."

STATUS_PATH="{{.status.applicationState.state}}"

while true; do
    STATE=$(kubectl get sparkapplication "$APP_NAME" \
        -n data-platform \
        -o jsonpath="$STATUS_PATH" 2>/dev/null || true)

    echo "SparkApplication state: $STATE"

    if [ "$STATE" = "COMPLETED" ]; then
        echo "Bronze -> Silver completed successfully."
        exit 0
    fi

    if [ "$STATE" = "FAILED" ]; then
        echo "Bronze -> Silver failed."

        kubectl get sparkapplication "$APP_NAME" \
            -n data-platform \
            -o yaml

        exit 1
    fi

    sleep 5
done
"""
        ],
        service_account_name="airflow-worker",
        is_delete_operator_pod=False,
        get_logs=True,
    )

    # ---------------------------------------------------------
    # Silver -> Gold
    # ---------------------------------------------------------

    submit_silver_to_gold = KubernetesPodOperator(
        task_id="silver_to_gold",
        name="submit-silver-to-gold",
        namespace="data-platform",
        image="bitnami/kubectl:latest",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"""
cat <<'EOF' | kubectl apply -f -
{SILVER_TO_GOLD_YAML}
EOF

APP_NAME="silver-to-gold-{{{{ ts_nodash | lower }}}}"

echo "Waiting for $APP_NAME to finish..."

STATUS_PATH="{{.status.applicationState.state}}"

while true; do
    STATE=$(kubectl get sparkapplication "$APP_NAME" \
        -n data-platform \
        -o jsonpath="$STATUS_PATH" 2>/dev/null || true)

    echo "SparkApplication state: $STATE"

    if [ "$STATE" = "COMPLETED" ]; then
        echo "Silver -> Gold completed successfully."
        exit 0
    fi

    if [ "$STATE" = "FAILED" ]; then
        echo "Silver -> Gold failed."

        kubectl get sparkapplication "$APP_NAME" \
            -n data-platform \
            -o yaml

        exit 1
    fi

    sleep 5
done
"""
        ],
        service_account_name="airflow-worker",
        is_delete_operator_pod=False,
        get_logs=True,
    )

    # ---------------------------------------------------------
    # Gold -> PostgreSQL
    # ---------------------------------------------------------

    submit_gold_to_postgres = KubernetesPodOperator(
        task_id="gold_to_postgres",
        name="submit-gold-to-postgres",
        namespace="data-platform",
        image="bitnami/kubectl:latest",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"""
cat <<'EOF' | kubectl apply -f -
{GOLD_TO_POSTGRES_YAML}
EOF

APP_NAME="gold-to-postgres-{{{{ ts_nodash | lower }}}}"

echo "Waiting for $APP_NAME to finish..."

STATUS_PATH="{{.status.applicationState.state}}"

while true; do
    STATE=$(kubectl get sparkapplication "$APP_NAME" \
        -n data-platform \
        -o jsonpath="$STATUS_PATH" 2>/dev/null || true)

    echo "SparkApplication state: $STATE"

    if [ "$STATE" = "COMPLETED" ]; then
        echo "Gold -> PostgreSQL completed successfully."
        exit 0
    fi

    if [ "$STATE" = "FAILED" ]; then
        echo "Gold -> PostgreSQL failed."

        kubectl get sparkapplication "$APP_NAME" \
            -n data-platform \
            -o yaml

        exit 1
    fi

    sleep 5
done
"""
        ],
        service_account_name="airflow-worker",
        is_delete_operator_pod=False,
        get_logs=True,
    )

    # ---------------------------------------------------------
    # Pipeline dependency
    # ---------------------------------------------------------

    (
        submit_bronze_to_silver
        >> submit_silver_to_gold
        >> submit_gold_to_postgres
    )
