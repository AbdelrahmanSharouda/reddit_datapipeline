# Reddit Data Pipeline

A data engineering project that sets up an end-to-end data pipeline for extracting, transforming, and loading (ETL) data from Reddit. This pipeline leverages various tools and techniques to collect data, process it, and store it for analysis.

Features
Data Extraction: Retrieves data from the Reddit API, supporting subreddit and post-specific filters.
Data Transformation: Cleans, structures, and formats the extracted data for consistency and usability.
Data Loading: Stores processed data in a chosen data warehouse for easy querying and analysis.
Scheduling: Configurable scheduling to automate pipeline runs.
Architecture
The project follows a modular ETL pipeline structure:

Extraction: Pulls data from Reddit’s API using pre-configured parameters.
Transformation: Processes raw data to ensure it meets analytical needs.
Loading: Stores data in a database or data warehouse.
Orchestration: Manages pipeline execution and dependencies.