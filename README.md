git@github.com:nngidi/Call-Data-Usage-Project.git

### Project Overview: Data Engineering Project  

This project focuses on integrating concepts and tools learned during the Data Engineering training to address real-world challenges. The emphasis is on combining technical skills, problem-solving, and teamwork to implement a resilient, cost-effective, and scalable data pipeline architecture. The tools and systems to be used include **Redpanda**, **Kafka**, **Debezium**, and **ScyllaDB**, among others, to manage, process, and analyze data streams effectively.

---

### Purpose  
The primary goal is to:
1. **Solve Complex Data Engineering Problems**: Utilize technical skills creatively to process, analyze, and store large volumes of data.  
2. **Collaborate**: Work in a team to design, implement, and present the solution.  
3. **Demonstrate Skills**: Build a fully operational data pipeline that integrates multiple systems and tools.  

---

### Core Technologies  
- **Redpanda**: A high-performance Kafka alternative for streaming and buffering data.  
- **Debezium**: For change data capture (CDC) to track updates from the CRM database.  
- **ScyllaDB**: A high-velocity store for real-time querying and storage of processed data.  
- **PostgreSQL**: For raw and transformed data storage.  
- **Docker**: To containerize and manage infrastructure components.  

---

### High-Level Objectives  
1. **Data Ingestion and Streaming**:  
   - Use **Redpanda** to stream data from the provided SFTP server and the CRM database using **Debezium**.  
   - Ensure seamless, scalable data flow with minimal latency.

2. **Stream Processing**:  
   - Summarize **Call Data Record (CDR)** information into daily usage statistics per MSISDN.  
   - Compute real-time metrics using **ScyllaDB** to handle high-velocity requirements.  

3. **Prepared Layers and Analytics**:  
   - Create intermediate data layers in PostgreSQL to enable efficient querying and reporting.   

4. **Real-Time API**:  
   - Expose summarized usage data through a secure REST API using **basic authentication**.   

---

### System Components and Deliverables  
1. **SFTP to Redpanda**: Automate the ingestion of raw CDR files from SFTP into Redpanda.  
2. **CDC with Debezium**: Stream changes from the CRM PostgreSQL database to Redpanda.  
3. **Real-Time Processing**: Use a stream-processing framework to compute daily summaries and store them in ScyllaDB.  
4. **High-Velocity Storage**: Implement ScyllaDB to manage low-latency read/write operations for processed data.  
5. **Usage API**: Provide RESTful endpoints for external users to query daily statistics securely.

---

### Getting Started  
1. **Setup Environment**:  
   - Install Docker, Git, and other dependencies.  
   - git@github.com:nngidi/Call-Data-Usage-Project.git

2. **Initialize Docker Environment**:  
   - Run `docker compose up -d` or equivalent to start all components.
