#!/bin/bash

# Configuration
LOG_FILE="/tmp/scd_passenger_$(date +%Y%m%d).log"
HIVE_SCRIPT="/hive/scripts/scd_dim_passenger.hql"
JDBC_URL="jdbc:hive2://localhost:10000/default"
USERNAME="hive"
PASSWORD="hive"
STAGING_PATH="/user/hive/staging/passenger_changes/$(date +%Y-%m-%d_%H)"
echo $STAGING_PATH
# Log start time
echo "Starting SCD Type 2 process at $(date)" | tee -a $LOG_FILE

# Make sure the script file exists
if [ ! -f "$HIVE_SCRIPT" ]; then
    echo "ERROR: Hive script file not found at $HIVE_SCRIPT" | tee -a $LOG_FILE
    exit 1
fi

# Execute the script using Beeline
echo "Executing Hive script using Beeline..." | tee -a $LOG_FILE
beeline -u "$JDBC_URL" -n "$USERNAME" -p "$PASSWORD" -f "$HIVE_SCRIPT" >> $LOG_FILE 2>&1

# Check exit status
if [ $? -eq 0 ]; then
    echo "SCD Type 2 process completed successfully at $(date)" | tee -a $LOG_FILE
    exit 0
else
    echo "ERROR: SCD Type 2 process failed at $(date)" | tee -a $LOG_FILE
    echo "Check $LOG_FILE for details." | tee -a $LOG_FILE
    exit 1
fi