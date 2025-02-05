#!/bin/bash

#set -x

# MySQL connection configuration
USER="root"
PASSWORD=""
HOST="127.0.0.1"
PORT="9030"  # Default port is 3306

# Get all databases
DATABASES=$(mysql -u$USER -h$HOST -P$PORT -e "SHOW DATABASES;" | tail -n +2)

# Iterate over each database
for DATABASE in $DATABASES; do
    # Get all tables
    TABLES=$(mysql -u$USER -h$HOST -P$PORT -D$DATABASE -e "SHOW TABLES;" | tail -n +2)
    
    # Iterate over each table
    for TABLE in $TABLES; do
        # Get the create table statement
        CREATE_TABLE_STMT=$(mysql -u$USER -h$HOST -P$PORT -D$DATABASE -e "SHOW CREATE TABLE $TABLE\G")
        
        # Check if it contains '"enable_unique_key_merge_on_write" = "true"'
        if echo "$CREATE_TABLE_STMT" | grep -q '"enable_unique_key_merge_on_write" = "true"'; then
            echo "$DATABASE.$TABLE" 
        fi
    done
done

