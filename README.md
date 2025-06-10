# Social Media Data ETL

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for social media data, enabling centralized analysis and automation.

## Overview

- **Extract** JSON data from multiple sources.
- **Transform** using Python libraries: Pandas and Numpy.
- **Load** into a PostgreSQL database.
- **Orchestrate** workflows with Apache Airflow.

## Data Files

1. **User**: User information (`_id` as primary key).
2. **Post**: User posts (`_id`, linked to user via `postOwner._id`).
3. **PostComments**: Comments on posts (`_id`, linked to user via `commentor._id`, and to post via `postId`).
4. **PostLikes**: Likes on posts (`_id`, linked to user via `liker._id`, and to post via `post._id`).

## Logical ERD Diagram

- **Users (MongoDB)** → **Posts (PostgreSQL)**: One-to-Many
- **Posts (PostgreSQL)** → **PostComments (MySQL)**: One-to-Many
- **Posts (PostgreSQL)** → **PostLikes (MySQL)**: One-to-Many
- **Users (MongoDB)** → **PostComments (MySQL)**: One-to-Many
- **Users (MongoDB)** → **PostLikes (MySQL)**: One-to-Many

## ETL Steps

1. **Extract**
    - Load User data into MongoDB.
    - Load Post data into PostgreSQL.
    - Load PostComments and PostLikes into MySQL.

2. **Transform**
    - Clean and integrate data using Pandas.
    - Apply Numpy for numerical/statistical transformations.

3. **Load**
    - Consolidate and load transformed data into PostgreSQL for analysis.

4. **Orchestration**
    - Use Apache Airflow to automate and monitor ETL workflows.

## Deliverables

- Logical ERD Diagram.
- Data loaded into MongoDB, PostgreSQL, and MySQL.
- ETL scripts for extraction, transformation, and loading.
- Apache Airflow DAGs for workflow orchestration.
