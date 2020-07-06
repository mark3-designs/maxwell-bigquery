#!/bin/bash


maxwell \
  --schema_user=mysql_username \
  --schema_password=pass \
  --schema_host=mysql.hostname \
  --replication_user=mysql_username \
  --replication_password=pass \
  --replication_host=mysql-replica \
  --replication_port=3306 \
  "$@"


exit
