#!/bin/bash

# Read from secure file
source .env.prod  # .env.prod is in .gitignore!

# Run with Java properties (more secure than env vars)
java -Ddb.password="$DB_PASSWORD" \
     -Daws.access.key="$AWS_ACCESS_KEY" \
     -Daws.secret.key="$AWS_SECRET_KEY" \
     -jar app.jar