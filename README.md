cat << 'EOF' > README.md
# TextAnalysisInTheCloud ☁️📄
**Distributed Text Analysis Pipeline on AWS**

**Author:** Swarka Bader

---

## 🚀 Overview

TextAnalysisInTheCloud is a distributed, fault-tolerant text analysis system built on AWS.  
It processes large text inputs using **Stanford CoreNLP** by distributing work across multiple
EC2 worker instances, coordinated via **SQS**, **S3**, and a central **Manager** service.

This project demonstrates:
- Cloud-native distributed design
- Message-driven orchestration (SQS)
- Fault tolerance & idempotency
- Horizontal scalability
- Real-world AWS deployment patterns

---

## 🧠 Architecture

### Local Client
- Uploads input text
- Submits a JOB request to the Manager
- Creates a job-specific response queue
- Waits for completion and downloads summary.html

### Manager (Orchestrator)
- Receives JOB requests
- Splits input into tasks
- Launches and manages workers
- Assigns tasks via SQS
- Collects results
- Backfills missing results from S3 if needed
- Builds final summary.html
- Sends DONE to the job’s private response queue

### Workers
- Poll task queue
- Download text chunks from S3
- Run:
  - POS tagging
  - Constituency parsing
  - Dependency parsing (CoreNLP)
- Upload results to S3
- Send RESULT messages back to Manager

---

## ☁️ AWS Services Used

- **EC2** – Manager & Worker instances  
- **SQS** – Job, task, result, and response queues  
- **S3** – Input storage, task outputs, final summary  
- **IAM** – Secure access via instance roles  

---

## 🧪 Sample Deployment

| Component | AMI | Instance Type |
|---------|-----|---------------|
| Manager | ami-0ae7afa7e641525b9 | r5.large |
| Workers | ami-0890c33a40574a343 | r5.large |

- Sample input: input-sample.txt
- Parallel tasks: 2
- End-to-end runtime: ~10 minutes

---

## ✅ Key Features

- Fault-tolerant task processing
- Idempotent workers and result handling
- Backfill recovery from S3
- Concurrent job support
- Horizontal worker scaling

---

## ⏱️ Timeouts & Visibility

- JOB visibility: **3600s**
- Worker task visibility: **1200s**
- Worker result visibility: **600s**

---

## 📂 Outputs

- Task results  
 
  s3://textanalysis-output-bucket/jobs/<jobId>/task-i.txt
  

- Final summary  
 
  s3://textanalysis-output-bucket/jobs/<jobId>/summary.html


---

## 🧑‍💻 Running Locally

### Set AWS Credentials
\`\`\`bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1
\`\`\`

### Build
mvn -pl manager -am clean package
mvn -pl worker  -am clean package
mvn -pl local   -am clean package

### Upload JARs
aws s3 cp manager/target/manager-1.0-SNAPSHOT.jar s3://textanalysis-jars-bucket/manager.jar
aws s3 cp worker/target/worker-1.0-SNAPSHOT.jar  s3://textanalysis-jars-bucket/worker.jar

### Run Local Client
java -jar local/target/local-1.0-SNAPSHOT.jar input.txt output.html 1 terminate


## 📜 Logs

- Manager: /home/ec2-user/manager.log
- Worker: /home/ec2-user/worker.log

---

## 🔐 Security

- IAM roles (no hardcoded credentials)
- Private S3 buckets and SQS queues
- Presigned URLs with limited lifetime

---

## 📈 Scalability

- Scale workers horizontally based on SQS depth
- Manager is lightweight orchestration only
- Can be extended with ASG, ECS, or EKS


