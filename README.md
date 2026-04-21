## STEDI_Human_Balance_Analytics
As a data engineer, I have been assigned a project to develop a data lakehouse solution built on AWS to process and curate sensor data for machine learning models that will be used by STEDI's data science team.
The STEDI Team has developed a hardware STEDI Step Trainer that:
*Trains users to perform balance exercises
*Records motion sensor data (distance detection)
*Paired with a mobile app that captures accelerometer data (X, Y, Z directions)

Privacy is a primary concern for STEDI, hence only customers who have explicitly agreed to share their data for research can have their data used for ML training.
I built an AWS data lakehouse pipeline using Spark and AWS Glue. The pipeline extracts, sanitizes, and curates sensor data so Data Scientists can train a step detection ML model.:
### Pipeline Steps
1. Ingests raw Step Trainer and accelerometer sensor data
2. Sanitizes customer data to filter only consenting users
3. Curates trusted datasets into zones (Landing → Trusted → Curated)
4. Delivers clean, query-ready data for ML model training
