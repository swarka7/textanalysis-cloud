# TextAnalysisInTheCloud

## Authors
- Swarka Bader — ID: *213402241*
- (Add any teammates if applicable)

## Overview
Distributed text analysis with three components:
- **Local**: uploads input, sends JOB to Manager, waits on its own response queue, downloads summary.
- **Manager**: launches workers, assigns tasks, collects results, backfills from S3 if RESULT is lost, builds `summary.html`, sends DONE.
- **Workers**: fetch tasks from SQS, download text, run POS/CONSTITUENCY/DEPENDENCY using CoreNLP, upload result to S3.

## Instances actually used (sample run)
- Manager: AMI `ami-0ae7afa7e641525b9`, type `r5.large`, role `LabRole`, key `vockey`.
- Workers: AMI `ami-0890c33a40574a343`, type `r5.large`, role `LabRole`, key `vockey`.
- Sample input: `input-sample.txt`, n=2; end-to-end time ~10 minutes 

### Running locally with AWS credentials (tester instructions)
Set AWS env vars in the shell (use your own keys/tokens):
```
$env:AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
$env:AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
$env:AWS_SESSION_TOKEN="YOUR_SESSION_TOKEN"   # omit if not using STS
$env:AWS_DEFAULT_REGION="us-east-1"
```
Then build/upload jars and run:
```
mvn -pl manager -am clean package
mvn -pl worker  -am clean package
mvn -pl local   -am clean package
aws s3 cp manager\target\manager-1.0-SNAPSHOT.jar s3://textanalysis-jars-bucket/manager.jar
aws s3 cp worker\target\worker-1.0-SNAPSHOT.jar  s3://textanalysis-jars-bucket/worker.jar
java -jar local\target\local-1.0-SNAPSHOT.jar local\src\main\resources\input-sample.txt local-output.html 1 (termiante) (optional)
```

## Multiple clients at once
- Each Local creates its own response queue (`textanalysis-local-resp-<jobId>`) and includes that URL in the JOB. DONE is sent only to that queue, so locals don’t interfere.
- Manager handles jobs in parallel. Tasks/results are keyed by jobId; no cross-job collisions.
- If running many locals, avoid using the `terminate` flag until all jobs are finished; send a single TERMINATE afterward.

## Behavior & Timeouts
- JOB visibility: 3600s to avoid re-delivery mid-job.
- Worker tasks visibility: 1200s; worker results visibility: 600s.
- Backfill: Manager backfills missing RESULTs by checking S3 for `task-i.txt` if RESULT is lost.
- Dedup: Manager ignores duplicate RESULTs; tasks are idempotent (same S3 key).
- Workers stream input line-by-line (no full-file buffering) to reduce RAM/GC overhead on smaller instances; pipelines are reused per worker.

## Failure Handling
- If a worker dies/stalls: task message becomes visible; another worker reprocesses (idempotent).
- RESULT lost: Manager backfills from S3 and completes.
- DONE queue missing: Manager still marks job complete (no re-run).
- Terminate: Manager stops new jobs; if queues are empty or activeJobs stuck, forces shutdown of workers and itself.

## Threads
- Manager uses a small thread pool to process incoming JOB messages concurrently while collecting results. It improves throughput when multiple locals run; not used for CPU-heavy work.
- Workers are single-threaded per instance (CoreNLP is heavy and gains little from parallel threads on small instances). Parallelism comes from multiple worker instances.

## Concurrency
- Multiple locals can run concurrently: tasks/results keyed by jobId; per-job response queues isolate DONE.
- Do not purge queues during runs. If you need a clean slate, purge once before starting jobs.

## Outputs
- Task outputs: `s3://textanalysis-output-bucket/jobs/<jobId>/task-i.txt`
- Summary: `s3://textanalysis-output-bucket/jobs/<jobId>/summary.html`

## Logs
- Manager: `/home/ec2-user/manager.log`
- Workers: `/home/ec2-user/worker.log`

## Security
- Use IAM roles/instance profiles (LabRole); no creds in code.
- Buckets/queues remain private; presigned URLs are limited duration (24h).

## Cleanup / Purge
- Only purge queues when no jobs are running:
```
aws sqs purge-queue --queue-url <manager|worker-tasks|worker-results|local-resp>
```
- Terminate manager/workers when done to avoid cost.

## Scalability Notes
- Horizontally scale workers based on SQS depth; jobs/tasks are independent and idempotent.
- For very high load, shard queues/managers; use auto-scaling (ASG/ECS/EKS) for workers.
- Manager work is orchestration only; workers do the heavy CPU. With enough workers, throughput scales with the number of instances. For very high client counts, run multiple managers, each with its own queues/buckets or namespacing.
