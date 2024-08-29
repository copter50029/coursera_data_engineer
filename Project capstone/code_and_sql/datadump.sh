#!/bin/bash

# MySQL database credentials
DB_USER="root"
DB_PASSWORD="z2PxD2cb3oubyNcgPmx8BcPV"
DB_NAME="sales"
DB_HOST="172.21.56.229"

# Output file
OUTPUT_FILE="sales_data.sql"

# Export the database to the output file
echo "Exporting database to $OUTPUT_FILE"
mysqldump -u $DB_USER -p$DB_PASSWORD -h $DB_HOST $DB_NAME > $OUTPUT_FILE

# Check if the export was successful
if [ $? -eq 0 ]; then
  echo "Database export successful!"
else
  echo "Database export failed!"
fi