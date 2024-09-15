## PROJECT INTRODUCTION

The City of New York is embarking on a project to integrate payroll data across all its
agencies. The City of New York would like to develop a Data Analytics platform to
accomplish two primary objectives:

• Financial Resource Allocation Analysis: Analyze how the City's financial resources are
allocated and how much of the City's budget is being devoted to overtime.

• Transparency and Public Accessibility: Make the data available to the interested public
to show how the City’s budget is being spent on salary and overtime pay for all
municipal employees.

### Project Goal

• Design a Data Warehouse for NYC
• Develop a scalable and automated ETL Pipeline to load the payroll data NYC data
warehouse
• Develop aggregate table(s) in the Data warehouse for easy analysis of the key business
questions
• Ensure quality and consistency of data in your pipeline
• Create a public user with limited privileges to enable public access to the NYC Data
warehouse
Properly document all your processes for reproducibility – version control

• Maintain a cloud hosted repository of your codebase to facilitate collaboration with
team members

### Data Architecture
![CHEESE!](diagrams\data architecture.jpg)

### Data modelling
![CHEESE!](diagrams\nyc Diagram.jpg)

### Technologies utilized
Vs-Code
GPC BigQuery
Buckets
Draw.io
Cloud-Composer
Airflow


### Step for step process of project execution

• Develop a schema showing the data models with draw.io
• Extracted the files from csv onto the python environment
• Loaded the raw files into the gcp bucket
• Performed tranformation on the files and uploaded to a cleaned folder in the buckets
• Loaded the raw files into the gcp bucket
• transfered cleaned files to a bigquery datawarehouse in a stagging environment

• Created stored procedures and created loaded aggregated data in a production dataset in the datawarehouse

• Set up airflow to schedule task through cloud composer

• Properly document all your processes for reproducibility – version control