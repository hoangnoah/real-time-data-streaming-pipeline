# real-time-data-streaming-pipeline
A real-time data processing pipeline that fetches user data from an API, processes it through Apache Kafka, transforms it using Apache Spark Streaming, and stores it in Apache Cassandra, orchestrated by Apache Airflow.

![image](https://github.com/user-attachments/assets/c7f87203-dea4-4bed-a0dc-8697d40a3adb)

## Main Tasks
The project aims to establish a real-time data analysis system for capturing and processing user data efficiently by leveraging advanced technologies and a robust data pipeline:
* Enhanced Data Capture: Real-time ingestion to capture user interactions as they happen, reducing latency.
* Scalable Architecture: A distributed system to handle growing data volumes seamlessly.
* Improved Data Quality: Ensuring integrity and accuracy through schema management and real-time validation.
* Streamlined Operations: Automating workflows to minimize manual intervention and reduce errors.
* Comprehensive Monitoring: Tools to monitor data stream health and resolve issues promptly.
* Business Integration: Real-time insights integrated into business applications for immediate action.
* Data-Driven Decisions: Providing stakeholders with timely, actionable insights for informed decision-making.

## Schedule data ingestion with airflow
Airflow automates and schedules workflows.
PostgreSQL stores processed data for further analysis.
![image](https://github.com/user-attachments/assets/3aebeb0d-9a38-4217-a3ae-066947d51619)

## View data in topic Confluent Control Center Kafka
Apache Kafka: Distributed messaging system for real-time data streaming.
Apache Kafka's Confluent Control center is a comprehensive management and monitoring tool for Kafka Clusters with user-friendly interface to track the health and performance of Kafka clusters, manage topics, configure alerts and analyze data streams.
Schema registry: Manages and enforces data schemas for Kafka topics, ensuring that data producers and consumers adhere to predefined data structures.
![image](https://github.com/user-attachments/assets/11aef9e5-b591-4221-b77f-b7a444fe7bb0)

## View Spark Job UI
Processes and analyzes streaming data.
![image](https://github.com/user-attachments/assets/e83d3643-befe-4290-be19-30f7508eef65)

## Data loaded in Cassandra
Cassandra: is an open source NoSQL distributed database, making it an excellent choice for real-time application.
Connect to Cassandra and run:
docker exec -it cassandra cqlsh -u cassandra -p cassandra
![image](https://github.com/user-attachments/assets/2cb43652-699d-4fda-a740-48f78e3d7b51)




