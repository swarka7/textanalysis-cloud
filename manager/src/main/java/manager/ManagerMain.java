package manager;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Base64;

public class ManagerMain {

    // Terminate only when terminate flag is requested (assignment semantics)
    private static final boolean ALWAYS_TERMINATE_AFTER_JOB = false;
    // Purge queues on startup to avoid stale messages from previous runs
    private static final boolean PURGE_WORKER_QUEUES_ON_START = true;
    private static final boolean PURGE_LOCAL_RESP_QUEUE_ON_START = true;
    // Default false to avoid deleting freshly-submitted JOBs if manager boots slightly after local
    private static final boolean PURGE_MANAGER_QUEUE_ON_START = false;

    // --------- AWS CONFIG ---------
    private static final Region REGION = Region.US_EAST_1;
    private static final Duration PRESIGN_DURATION = Duration.ofHours(24);
    private static final int MAX_WORKERS = 19;
    // Keep SQS messages invisible long enough to avoid redelivery for long parses
    // Keep JOB messages invisible long enough so long-running jobs don't get redelivered
    private static final int MANAGER_QUEUE_VISIBILITY_SECONDS = 3600;
    private static final int RESULT_QUEUE_VISIBILITY_SECONDS = 600;

    // S3 buckets
    private static final String INPUT_BUCKET  = "textanalysis-input-bucket";
    private static final String OUTPUT_BUCKET = "textanalysis-output-bucket";

    // Local <-> Manager queues
    // LocalMain sends JOB messages to this queue
    private static final String LOCAL_TO_MANAGER_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-manager-queue";

    // Response queue for Local – usually same as LOCAL_RESP_QUEUE_URL in LocalMain
    // (we still read the actual URL from the message body, in case you change it).
    @SuppressWarnings("unused")
    private static final String MANAGER_TO_LOCAL_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-local-resp-queue";

    // Manager <-> Worker queues
    private static final String WORKER_TASK_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-worker-tasks";

    private static final String WORKER_RESULTS_QUEUE_URL =
            "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-worker-results";

    // EC2 tags & configuration
    private static final String MANAGER_TAG_KEY   = "Role";
    private static final String MANAGER_TAG_VALUE = "Manager";

    private static final String WORKER_TAG_KEY   = "Role";
    private static final String WORKER_TAG_VALUE = "Worker";

    private static final String WORKER_SECURITY_GROUP_ID = "sg-0209012c37e166da0";

    // AMI to boot workers (Amazon Linux 2)
    private static final String WORKER_AMI_ID = "ami-0890c33a40574a343";

    // Where we expect jars to live in S3 (for userData scripts)
    private static final String JARS_BUCKET = "textanalysis-jars-bucket";

    // IAM instance profile and keypair for workers so they can reach SQS/S3
    private static final String WORKER_IAM_PROFILE = "LabInstanceProfile";
    private static final String WORKER_KEY_NAME = "vockey";


    public static void main(String[] args) throws Exception {
        System.out.println("Manager starting...");

        SqsClient sqs = SqsClient.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        S3Client s3 = S3Client.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        Ec2Client ec2 = Ec2Client.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        purgeQueuesOnStart(sqs);

        S3Presigner presigner = S3Presigner.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicBoolean terminateRequested = new AtomicBoolean(false);
        AtomicInteger activeJobs = new AtomicInteger(0);

        boolean terminateAfterJob = false;

        // Watchdog: if terminate was requested and no active jobs, force shutdown
        Thread terminator = new Thread(() -> {
            while (true) {
                if (terminateRequested.get() && activeJobs.get() == 0) {
                    System.out.println("Terminate watchdog: shutting down workers and manager.");
                    terminateAllWorkers(ec2);
                    selfTerminateManager(ec2);
                    executor.shutdown();
                    System.exit(0);
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                }
            }
        });
        terminator.setDaemon(true);
        terminator.start();

        // ------------- MAIN LOOP: handle jobs from Local apps -------------
        while (true) {
            if (terminateRequested.get()) {
                // If no active jobs, shut down
                if (activeJobs.get() == 0) {
                    System.out.println("Terminate requested; no active jobs. Shutting down workers and manager.");
                    terminateAllWorkers(ec2);
                    selfTerminateManager(ec2);
                    executor.shutdown();
                    return;
                }
                // Additionally, if queues are empty, force shutdown even if activeJobs is stuck
                if (queuesEmpty(sqs)) {
                    System.out.println("Terminate requested; queues empty. Forcing shutdown of manager/workers.");
                    terminateAllWorkers(ec2);
                    selfTerminateManager(ec2);
                    executor.shutdown();
                    return;
                }
                // Do not accept new jobs after terminate; just wait
                try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                continue;
            }

            ReceiveMessageRequest recvReq = ReceiveMessageRequest.builder()
                    .queueUrl(LOCAL_TO_MANAGER_QUEUE_URL)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    // Avoid duplicate JOB processing due to short visibility
                    .visibilityTimeout(MANAGER_QUEUE_VISIBILITY_SECONDS)
                    .build();

            List<Message> msgs = sqs.receiveMessage(recvReq).messages();
            if (msgs.isEmpty()) {
                System.out.println("No messages from local yet...");
                continue;
            }

            for (Message m : msgs) {
                String body = m.body();
                System.out.println("Got message from local: " + body);

                if (body.startsWith("TERMINATE")) {
                    terminateRequested.set(true);
                    deleteMessage(sqs, LOCAL_TO_MANAGER_QUEUE_URL, m);
                    System.out.println("Terminate message received. Will stop accepting new jobs after current ones finish.");
                    continue;
                }

                String[] parts = body.split("#");
                if (parts.length < 9 || !"JOB".equals(parts[0])) {
                    System.out.println("Unknown or malformed message, deleting: " + body);
                    deleteMessage(sqs, LOCAL_TO_MANAGER_QUEUE_URL, m);
                    continue;
                }

                // JOB#jobId#inBucket#inKey#outBucket#summaryKey#n#terminate#respQueue
                String jobId        = parts[1];
                String inBucket     = parts[2];
                String inKey        = parts[3];
                String outBucket    = parts[4];
                String summaryKey   = parts[5];
                int n               = Integer.parseInt(parts[6]);
                boolean terminate   = Boolean.parseBoolean(parts[7]);
                String respQueueUrl = parts[8];

                boolean requestTerminate = terminate || ALWAYS_TERMINATE_AFTER_JOB;
                activeJobs.incrementAndGet();

                executor.submit(() -> {
                    boolean success = false;
                    try {
                        handleJob(
                                sqs, s3, ec2, presigner,
                                jobId, inBucket, inKey,
                                outBucket, summaryKey,
                                n, respQueueUrl
                        );
                        System.out.println("Job " + jobId + " done.");
                        success = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (success) {
                            deleteMessage(sqs, LOCAL_TO_MANAGER_QUEUE_URL, m);
                        } else {
                            System.out.println("Job failed; leaving SQS message for retry: " + jobId);
                        }

                        if (requestTerminate && success) {
                            terminateRequested.set(true);
                        }

                        int remaining = activeJobs.decrementAndGet();
                        if (terminateRequested.get() && remaining == 0) {
                            System.out.println("Terminate requested and no active jobs. Shutting down workers and manager.");
                            terminateAllWorkers(ec2);
                            selfTerminateManager(ec2);
                            executor.shutdown();
                            System.exit(0);
                        }
                    }
                });
            }

            if (terminateRequested.get() && activeJobs.get() == 0) {
                System.out.println("Terminate requested and no active jobs. Shutting down workers and manager.");
                terminateAllWorkers(ec2);
                selfTerminateManager(ec2);
                executor.shutdown();
                return;
            }
        }
    }

    // -------------------- JOB HANDLING --------------------

    private static void handleJob(
            SqsClient sqs,
            S3Client s3,
            Ec2Client ec2,
            S3Presigner presigner,
            String jobId,
            String inBucket,
            String inKey,
            String outBucket,
            String summaryKey,
            int n,
            String respQueueUrl) throws IOException {

        System.out.println("Handling job " + jobId +
                " input=" + inBucket + "/" + inKey +
                " outputSummary=" + outBucket + "/" + summaryKey +
                " n=" + n);

        // 1) Download the input file from S3
        Path tempDir = Files.createTempDirectory("job-" + jobId);
        Path localInput = tempDir.resolve("input.txt");

        downloadFromS3(s3, inBucket, inKey, localInput);

        // Each line: <ANALYSIS_TYPE>\t<URL>
        List<String> lines = Files.readAllLines(localInput, StandardCharsets.UTF_8)
                .stream()
                .filter(l -> !l.trim().isEmpty())
                .collect(Collectors.toList());

        int totalTasks = lines.size();
        if (totalTasks == 0) {
            System.out.println("No lines in job file, writing empty summary.");
            writeEmptySummaryAndNotify(s3, sqs, jobId, outBucket, summaryKey, respQueueUrl);
            return;
        }

        // 2) Ensure we have enough workers for this job (ceil(totalTasks / n), capped)
        int desiredWorkers = Math.min(MAX_WORKERS,
                Math.max(1, (int) Math.ceil(totalTasks / (double) n)));
        ensureWorkersRunning(ec2, desiredWorkers);

        // 3) Send tasks to worker queue
        System.out.println("Sending " + totalTasks + " tasks to workers.");

        for (int i = 0; i < totalTasks; i++) {
            String line = lines.get(i);
            String[] parts = line.split("\\t");
            if (parts.length < 2) {
                System.out.println("Bad line (missing tab): " + line);
                continue;
            }
            String analysisType = parts[0].trim();  // POS | CONSTITUENCY | DEPENDENCY
            String textUrl      = parts[1].trim();

            String taskId = "task-" + i;

            String resultKey = "jobs/" + jobId + "/task-" + i + ".txt";

            String taskBody = String.join("#",
                    "TASK",
                    jobId,
                    taskId,
                    analysisType,
                    textUrl,
                    outBucket,
                    resultKey
            );

            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(WORKER_TASK_QUEUE_URL)
                    .messageBody(taskBody)
                    .build());
        }

        // 4) Collect results
        Map<String, TaskResult> results = new HashMap<>();
        System.out.println("Waiting for " + totalTasks + " worker results for job " + jobId);

        long lastMissingLog = System.currentTimeMillis();
        while (results.size() < totalTasks) {
            ReceiveMessageRequest recvReq = ReceiveMessageRequest.builder()
                    .queueUrl(WORKER_RESULTS_QUEUE_URL)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    // Keep RESULT messages hidden long enough to process/delete them
                    .visibilityTimeout(RESULT_QUEUE_VISIBILITY_SECONDS)
                    .build();

            List<Message> msgs = sqs.receiveMessage(recvReq).messages();
            if (msgs.isEmpty()) {
                System.out.println("No worker results yet (" + results.size() + "/" + totalTasks + ")");
                continue;
            }

            for (Message m : msgs) {
                String body = m.body();
                // Keep trailing empty detail field so OK results are accepted
                String[] parts = body.split("#", -1);

                // RESULT#jobId#taskId#bucket#key#status#detail
                if (parts.length < 6 || !"RESULT".equals(parts[0])) {
                    deleteMessage(sqs, WORKER_RESULTS_QUEUE_URL, m);
                    continue;
                }

                String msgJobId   = parts[1];
                String taskId     = parts[2];
                String bucket     = parts[3];
                String key        = parts[4];
                String status     = parts[5];
                String detail     = parts.length > 6 ? parts[6] : "";

                if (!jobId.equals(msgJobId)) {
                    // Result for another job; leave it in the queue or handle separately.
                    continue;
                }

                // Deduplicate in case a task result is redelivered after visibility timeout
                if (results.containsKey(taskId)) {
                    deleteMessage(sqs, WORKER_RESULTS_QUEUE_URL, m);
                    continue;
                }

                results.put(taskId, new TaskResult(taskId, bucket, key, status, detail));
                deleteMessage(sqs, WORKER_RESULTS_QUEUE_URL, m);
                System.out.println("Got RESULT for job " + jobId + ", task " + taskId +
                        " (" + results.size() + "/" + totalTasks + ")");
            }

            // If still missing, see if the outputs exist in S3 (RESULT message may have been lost)
            if (results.size() < totalTasks) {
                fillMissingFromS3(s3, jobId, outBucket, totalTasks, results);
            }

            if (results.size() < totalTasks && System.currentTimeMillis() - lastMissingLog > 5000) {
                System.out.println("No worker results yet (" + results.size() + "/" + totalTasks + ")");
                lastMissingLog = System.currentTimeMillis();
            }
        }

        // 5) Build summary.html locally and upload to S3
        Path summaryLocal = tempDir.resolve("summary.html");
        buildSummaryFile(lines, results, summaryLocal, presigner);

        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(outBucket)
                .key(summaryKey)
                .contentType("text/html")
                .build();
        s3.putObject(putReq, summaryLocal);

        // 6) Notify Local app with DONE message
        String doneBody = String.join("#",
                "DONE",
                jobId,
                outBucket,
                summaryKey
        );

        try {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(respQueueUrl)
                    .messageBody(doneBody)
                    .build());
            System.out.println("DONE message sent to " + respQueueUrl + " for job " + jobId);
        } catch (SqsException e) {
            // If the response queue is gone (e.g., purged/deleted), treat the job as completed
            System.out.println("DONE send failed (will still mark job complete): " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            System.out.println("DONE send failed (will still mark job complete): " + e.getMessage());
        }
    }

    // -------------------- WORKER POOL MANAGEMENT --------------------

    private static void ensureWorkersRunning(Ec2Client ec2, int desiredWorkers) {
        // Count existing running/pending worker instances
        DescribeInstancesRequest req = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:" + WORKER_TAG_KEY)
                                .values(WORKER_TAG_VALUE).build(),
                        Filter.builder().name("instance-state-name")
                                .values("pending", "running").build()
                )
                .build();

        DescribeInstancesResponse resp = ec2.describeInstances(req);

        long current = resp.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .count();

        int toLaunch = (int) Math.max(0, desiredWorkers - current);

        if (toLaunch <= 0) {
            System.out.println("Workers already running: " + current);
            return;
        }

        System.out.println("Launching " + toLaunch + " worker instances.");

        String userDataScript =
                "#!/bin/bash\n" +
                        "cd /home/ec2-user\n" +
                        "aws s3 cp s3://" + JARS_BUCKET + "/worker.jar /home/ec2-user/worker.jar\n" +
                        "chown ec2-user:ec2-user /home/ec2-user/worker.jar\n" +
                        "sudo -u ec2-user nohup java -Xmx4096m -jar /home/ec2-user/worker.jar > /home/ec2-user/worker.log 2>&1 &\n";

        String userDataBase64 = Base64.getEncoder()
                .encodeToString(userDataScript.getBytes(StandardCharsets.UTF_8));

        RunInstancesRequest runReq = RunInstancesRequest.builder()
                .imageId(WORKER_AMI_ID)
                .instanceType(InstanceType.R5_LARGE)
                .keyName(WORKER_KEY_NAME)
                .minCount(toLaunch)
                .maxCount(toLaunch)
                .securityGroupIds(WORKER_SECURITY_GROUP_ID)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name(WORKER_IAM_PROFILE)
                        .build())
                .userData(userDataBase64)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder()
                                .key(WORKER_TAG_KEY)
                                .value(WORKER_TAG_VALUE)
                                .build())
                        .build())
                .build();

        ec2.runInstances(runReq);
    }

    private static void terminateAllWorkers(Ec2Client ec2) {
        DescribeInstancesRequest req = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:" + WORKER_TAG_KEY)
                                .values(WORKER_TAG_VALUE).build(),
                        Filter.builder().name("instance-state-name")
                                .values("pending", "running", "stopping", "stopped").build()
                )
                .build();

        DescribeInstancesResponse resp = ec2.describeInstances(req);

        List<String> ids = resp.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .map(Instance::instanceId)
                .collect(Collectors.toList());

        if (ids.isEmpty()) {
            System.out.println("No workers to terminate.");
            return;
        }

        ec2.terminateInstances(TerminateInstancesRequest.builder()
                .instanceIds(ids)
                .build());

        System.out.println("Terminate requested for workers: " + ids);
    }

    private static Optional<String> fetchInstanceIdFromImdsV2() {
        try {
            HttpURLConnection tokenConn = (HttpURLConnection) new URL("http://169.254.169.254/latest/api/token").openConnection();
            tokenConn.setRequestMethod("PUT");
            tokenConn.setRequestProperty("X-aws-ec2-metadata-token-ttl-seconds", "21600");
            tokenConn.setConnectTimeout(2000);
            tokenConn.setReadTimeout(2000);
            tokenConn.connect();
            if (tokenConn.getResponseCode() != 200) {
                return Optional.empty();
            }
            String token = new String(tokenConn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

            HttpURLConnection mdConn = (HttpURLConnection) new URL("http://169.254.169.254/latest/meta-data/instance-id").openConnection();
            mdConn.setRequestProperty("X-aws-ec2-metadata-token", token);
            mdConn.setConnectTimeout(2000);
            mdConn.setReadTimeout(2000);
            if (mdConn.getResponseCode() != 200) {
                return Optional.empty();
            }
            String instanceId = new String(mdConn.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return instanceId.isEmpty() ? Optional.empty() : Optional.of(instanceId);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static void selfTerminateManager(Ec2Client ec2) {
        Optional<String> instanceIdOpt = fetchInstanceIdFromImdsV2();
        if (instanceIdOpt.isEmpty()) {
            System.out.println("Self-termination skipped: instance-id not available from IMDS (IMDSv2 token required?).");
            return;
        }

        String instanceId = instanceIdOpt.get();
        try {
            ec2.terminateInstances(TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build());
            System.out.println("Terminate requested for manager instance: " + instanceId);
        } catch (Exception e) {
            System.out.println("Self-termination failed for manager instance " + instanceId + ": " + e.getMessage());
        }
    }

    // -------------------- SUMMARY BUILDING --------------------

    private static void buildSummaryFile(List<String> inputLines,
                                         Map<String, TaskResult> results,
                                         Path outHtml,
                                         S3Presigner presigner) throws IOException {

        try (BufferedWriter w = Files.newBufferedWriter(outHtml, StandardCharsets.UTF_8)) {
            w.write("<html><body>\n");
            w.write("<h1>Text Analysis Summary</h1>\n");
            w.write("<ul>\n");

            for (int i = 0; i < inputLines.size(); i++) {
                String line = inputLines.get(i);
                String[] parts = line.split("\\t");
                String analysisType = parts[0].trim();
                String inputUrl     = parts.length > 1 ? parts[1].trim() : "(missing url)";

                String taskId = "task-" + i;
                TaskResult r  = results.get(taskId);

                w.write("<li>");
                w.write(analysisType + ": ");
                w.write("<a href=\"" + escapeHtml(inputUrl) + "\">input</a> ");

                if (r == null || !"OK".equals(r.status)) {
                    String detail = (r == null) ? "No result" : r.detail;
                    w.write("(error: " + escapeHtml(detail) + ")");
                } else {
                    String outUrl = presign(presigner, r.bucket, r.key);
                    w.write("<a href=\"" + escapeHtml(outUrl) + "\">output</a>");
                }

                w.write("</li>\n");
            }

            w.write("</ul>\n");
            w.write("</body></html>\n");
        }
    }

    private static String escapeHtml(String s) {
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private static void writeEmptySummaryAndNotify(S3Client s3,
                                                   SqsClient sqs,
                                                   String jobId,
                                                   String bucket,
                                                   String key,
                                                   String respQueueUrl) throws IOException {
        Path tmp = Files.createTempFile("summary-empty", ".html");
        Files.writeString(tmp, "<html><body>No entries.</body></html>",
                StandardCharsets.UTF_8);

        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("text/html")
                .build();
        s3.putObject(putReq, tmp);

        String doneBody = String.join("#",
                "DONE",
                jobId,
                bucket,
                key
        );

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(respQueueUrl)
                .messageBody(doneBody)
                .build());
    }

    // -------------------- S3 & SQS HELPERS --------------------
    /**
     * Backfill missing results by checking if the expected task files already exist in S3
     * (covers the case where the RESULT message was lost but the worker uploaded successfully).
     */
    private static void fillMissingFromS3(S3Client s3,
                                          String jobId,
                                          String outBucket,
                                          int totalTasks,
                                          Map<String, TaskResult> results) {
        for (int i = 0; i < totalTasks; i++) {
            String taskId = "task-" + i;
            if (results.containsKey(taskId)) continue;
            String key = "jobs/" + jobId + "/task-" + i + ".txt";
            try {
                HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder()
                        .bucket(outBucket)
                        .key(key)
                        .build());
                if (head.contentLength() != null && head.contentLength() > 0) {
                    results.put(taskId, new TaskResult(taskId, outBucket, key, "OK", ""));
                    System.out.println("Backfilled RESULT from S3 for job " + jobId + ", task " + taskId);
                }
            } catch (S3Exception e) {
                // Ignore missing object
            } catch (Exception e) {
                System.out.println("Backfill check failed for " + key + ": " + e.getMessage());
            }
        }
    }

    private static void downloadFromS3(S3Client s3,
                                       String bucket,
                                       String key,
                                       Path localPath) throws IOException {
        Files.createDirectories(localPath.getParent());

        GetObjectRequest getReq = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        try (InputStream in = s3.getObject(getReq)) {
            Files.copy(in, localPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void deleteMessage(SqsClient sqs, String queueUrl, Message m) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build());
    }

    private static void purgeQueuesOnStart(SqsClient sqs) {
        if (PURGE_MANAGER_QUEUE_ON_START) {
            tryPurge(sqs, LOCAL_TO_MANAGER_QUEUE_URL, "manager queue");
        }
        if (PURGE_WORKER_QUEUES_ON_START) {
            tryPurge(sqs, WORKER_TASK_QUEUE_URL, "worker tasks queue");
            tryPurge(sqs, WORKER_RESULTS_QUEUE_URL, "worker results queue");
        }
        if (PURGE_LOCAL_RESP_QUEUE_ON_START) {
            tryPurge(sqs, MANAGER_TO_LOCAL_QUEUE_URL, "local response queue");
        }
    }

    private static void tryPurge(SqsClient sqs, String queueUrl, String label) {
        try {
            sqs.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
            System.out.println("Purged " + label + ": " + queueUrl);
        } catch (Exception e) {
            System.out.println("Purge skipped for " + label + " (" + e.getMessage() + ")");
        }
    }

    private static boolean queuesEmpty(SqsClient sqs) {
        try {
            Map<QueueAttributeName, String> m1 = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(LOCAL_TO_MANAGER_QUEUE_URL)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                    .build()).attributes();
            Map<QueueAttributeName, String> m2 = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(WORKER_TASK_QUEUE_URL)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                    .build()).attributes();
            Map<QueueAttributeName, String> m3 = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(WORKER_RESULTS_QUEUE_URL)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                    .build()).attributes();
            return isZero(m1) && isZero(m2) && isZero(m3);
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isZero(Map<QueueAttributeName, String> attrs) {
        String vis = attrs.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0");
        String invis = attrs.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0");
        try {
            return Integer.parseInt(vis) == 0 && Integer.parseInt(invis) == 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static String presign(S3Presigner presigner, String bucket, String key) {
        GetObjectRequest getReq = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        GetObjectPresignRequest presignReq = GetObjectPresignRequest.builder()
                .signatureDuration(PRESIGN_DURATION)
                .getObjectRequest(getReq)
                .build();
        return presigner.presignGetObject(presignReq).url().toString();
    }

    // -------------------- INTERNAL TYPES --------------------
    public static List<Task> parseInputFile(Path inputFile) {
        List<Task> tasks = new ArrayList<>();
        try {
            List<String> lines = Files.readAllLines(inputFile, StandardCharsets.UTF_8);
            int index = 0;
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split("\\t");
                if (parts.length != 2) {
                    System.err.println("Skipping malformed line: " + line);
                    continue;
                }
                String analysisType = parts[0].trim();
                String url = parts[1].trim();
                if (!List.of("POS", "CONSTITUENCY", "DEPENDENCY").contains(analysisType)) {
                    System.err.println("Skipping invalid analysis type: " + line);
                    continue;
                }
                tasks.add(new Task(index++, analysisType, url));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tasks;
    }

    public static class Task {
        public final int lineNumber;
        public final String type;
        public final String url;
        public Task(int lineNumber, String type, String url) {
            this.lineNumber = lineNumber;
            this.type = type;
            this.url = url;
        }
    }

    private static class TaskResult {
        final String taskId;
        final String bucket;
        final String key;
        final String status;  // OK / ERROR
        final String detail;  // error text

        TaskResult(String taskId, String bucket, String key, String status, String detail) {
            this.taskId = taskId;
            this.bucket = bucket;
            this.key = key;
            this.status = status;
            this.detail = detail;
        }
    }
}
