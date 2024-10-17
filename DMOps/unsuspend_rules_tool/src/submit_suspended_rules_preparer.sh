#!/bin/bash
# shellcheck disable=SC1090

# Load common utilities
set -e
script_dir="$(cd "$(dirname "$0")" && pwd)"
. /data/CMSSpark/bin/utils/common_utils.sh

# Initiate Kerberos Credentials
#!/bin/sh

if [ -z "$KERBEROS_ACC" ]; then
  echo "Error: KERBEROS_ACC is not set"
  exit 1
fi

if [ ! -f "/etc/secrets/${KERBEROS_ACC}.keytab" ]; then
  echo "Error: Keytab file /etc/secrets/${KERBEROS_ACC}.keytab not found"
  exit 1
fi

kinit -kt "/etc/secrets/${KERBEROS_ACC}.keytab" "${KERBEROS_ACC}"


# Setup Spark for a k8s cluster
util_setup_spark_k8s

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.shuffle.useOldFetchProtocol=true --conf "spark.driver.bindAddress=0.0.0.0"
    --conf spark.shuffle.service.enabled=true --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${DRIVERPORT}" --conf "spark.driver.blockManager.port=${BMPORT}"
    --driver-memory=32g --num-executors 30 --executor-memory=32g --packages org.apache.spark:spark-avro_2.12:3.4.0
    --py-files "/$script_dir/cmsmonitoring.zip,/$script_dir/stomp.zip"
)

#Log all to stdout
spark-submit "${spark_submit_args[@]}" "$script_dir/suspend_rules_preparer.py" 2>&1