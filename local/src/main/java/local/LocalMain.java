package local;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.Base64;

public class LocalMain {

    private static final Region REGION = Region.US_EAST_1;

    private static final String INPUT_BUCKET = "textanalysis-input-bucket";
    private static final String OUTPUT_BUCKET = "textanalysis-output-bucket";
    private static final String JARS_BUCKET = "textanalysis-jars-bucket";

    private static final String MANAGER_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-manager-queue";
    private static final String LOCAL_RESP_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-local-resp-queue";

    // EC2 launch details for manager
    private static final String MANAGER_AMI_ID = "ami-0ae7afa7e641525b9";
    private static final String MANAGER_SECURITY_GROUP_ID = "sg-0209012c37e166da0";
    private static final String MANAGER_IAM_PROFILE = "LabInstanceProfile";
    private static final String MANAGER_KEY_NAME = "vockey"; // existing lab keypair

    private static final String MANAGER_TAG_KEY = "Role";
    private static final String MANAGER_TAG_VALUE = "Manager";

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java -jar local.jar <inputFile> <outputFile> <n> [terminate]");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];
        String n = args[2];
        boolean terminate = (args.length > 3 && args[3].equalsIgnoreCase("terminate"));

        String jobId = "job-" + UUID.randomUUID();

        System.out.println("Starting Local app with jobId = " + jobId);

        Ec2Client ec2 = Ec2Client.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        S3Client s3 = S3Client.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        SqsClient sqs = SqsClient.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        // 1. Ensure Manager is running (skip if unauthorized from this environment)
        try {
            ensureManagerRunning(ec2);
        } catch (Ec2Exception e) {
            System.err.println("Warning: ensureManagerRunning skipped (" + e.awsErrorDetails().errorCode() + "): " + e.getMessage());
            // proceed assuming manager is already running
        } catch (Exception e) {
            System.err.println("Warning: ensureManagerRunning skipped: " + e.getMessage());
            // proceed assuming manager is already running
        }

        // 2. Upload input file to S3
        String inputKey = "jobs/" + jobId + "/input.txt";
        uploadToS3(s3, INPUT_BUCKET, inputKey, Paths.get(inputFile));

        // 3. Compute where we expect the Manager to write the summary
        String summaryKey = "jobs/" + jobId + "/summary.html";

        // 4. Create a dedicated response queue for this job
        String respQueueUrl = createResponseQueue(sqs, jobId);

        // 5. Send message to Manager queue
        sendJobMessage(sqs, jobId, n, terminate, inputKey, summaryKey, respQueueUrl);

        // 6. Wait for DONE message with this jobId
        waitForResultAndDownload(sqs, s3, jobId, summaryKey, Paths.get(outputFile), respQueueUrl);

        // 7. Cleanup the response queue
        deleteQueueQuietly(sqs, respQueueUrl);

        System.out.println("Job " + jobId + " completed. Summary saved to " + outputFile);
    }

    /** -------------- EC2 / Manager helpers -------------- */

    private static void ensureManagerRunning(Ec2Client ec2) {
        // 1. Look for running instance tagged as Manager
        DescribeInstancesRequest req = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("tag:" + MANAGER_TAG_KEY)
                                .values(MANAGER_TAG_VALUE)
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("pending", "running")
                                .build()
                )
                .build();

        DescribeInstancesResponse resp = ec2.describeInstances(req);

        boolean found = resp.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .anyMatch(i -> i.state().nameAsString().equals("running")
                        || i.state().nameAsString().equals("pending"));

        if (found) {
            System.out.println("Manager already running.");
            return;
        }

        System.out.println("Manager not running. Starting a new Manager instance...");

        String userData = "#!/bin/bash\n" +
                "aws configure set region us-east-1\n" +
                "cd /home/ec2-user\n" +
                "aws s3 cp s3://" + JARS_BUCKET + "/manager.jar /home/ec2-user/manager.jar\n" +
                "chown ec2-user:ec2-user /home/ec2-user/manager.jar\n" +
                "sudo -u ec2-user nohup java -Xmx4g -jar /home/ec2-user/manager.jar > /home/ec2-user/manager.log 2>&1 &\n";

        RunInstancesRequest runReq = RunInstancesRequest.builder()
                .imageId(MANAGER_AMI_ID)
                .instanceType(InstanceType.R5_LARGE)
                .keyName(MANAGER_KEY_NAME)
                .minCount(1)
                .maxCount(1)
                .securityGroupIds(MANAGER_SECURITY_GROUP_ID)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name(MANAGER_IAM_PROFILE)
                        .build())
                .userData(Base64.getEncoder().encodeToString(userData.getBytes()))
                .tagSpecifications(
                        TagSpecification.builder()
                                .resourceType(ResourceType.INSTANCE)
                                .tags(software.amazon.awssdk.services.ec2.model.Tag.builder()
                                        .key(MANAGER_TAG_KEY)
                                        .value(MANAGER_TAG_VALUE)
                                        .build())
                                .build()
                )
                .build();

        ec2.runInstances(runReq);
        System.out.println("Manager instance start requested (pending/running).");
    }

    /** -------------- S3 helpers -------------- */

    private static void uploadToS3(S3Client s3, String bucket, String key, Path file) throws IOException {
        System.out.println("Uploading input " + file + " to s3://" + bucket + "/" + key);
        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3.putObject(putReq, file);
    }

    /** -------------- SQS helpers -------------- */

    private static void sendJobMessage(SqsClient sqs,
                                       String jobId,
                                       String n,
                                       boolean terminate,
                                       String inputKey,
                                       String summaryKey,
                                       String respQueueUrl) {
        String body = String.join("#",
                "JOB",
                jobId,
                INPUT_BUCKET,
                inputKey,
                OUTPUT_BUCKET,
                summaryKey,
                n,
                Boolean.toString(terminate),
                respQueueUrl);

        SendMessageRequest sendReq = SendMessageRequest.builder()
                .queueUrl(MANAGER_QUEUE_URL)
                .messageBody(body)
                .build();

        sqs.sendMessage(sendReq);
        System.out.println("Sent job message to manager: " + body);
    }

    private static void waitForResultAndDownload(SqsClient sqs,
                                                 S3Client s3,
                                                 String jobId,
                                                 String summaryKey,
                                                 Path localOutput,
                                                 String respQueueUrl) throws IOException {

        System.out.println("Waiting for DONE message for jobId=" + jobId);

        while (true) {
            ReceiveMessageRequest recvReq = ReceiveMessageRequest.builder()
                    .queueUrl(respQueueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20) // long polling
                    .build();

            List<Message> msgs = sqs.receiveMessage(recvReq).messages();

            if (msgs.isEmpty()) {
                System.out.println("No messages yet...");
                continue;
            }

            for (Message m : msgs) {
                String body = m.body();
                String[] parts = body.split("#");

                if (parts.length >= 3 && parts[0].equals("DONE") && parts[1].equals(jobId)) {
                    // DONE#jobId#bucket#key
                    String bucket = parts[2];
                    String key = parts[3];

                    System.out.println("Got DONE for job " + jobId + ": s3://" + bucket + "/" + key);

        // delete message from queue
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(respQueueUrl)
                            .receiptHandle(m.receiptHandle())
                            .build());

                    // download summary file
                    downloadFromS3(s3, bucket, key, localOutput);
                    return;
                } else {
                    // Message for another job – leave it or delete it based on your design
                }
            }
        }
    }

    private static void downloadFromS3(S3Client s3, String bucket, String key, Path localOutput) throws IOException {
        System.out.println("Downloading summary to " + localOutput);

        GetObjectRequest getReq = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        if (localOutput.getParent() != null) {
            Files.createDirectories(localOutput.getParent());
        }

        // Overwrite if the file already exists to avoid FileAlreadyExistsException
        Files.deleteIfExists(localOutput);

        s3.getObject(getReq, localOutput);
    }

    /** Create a per-job response queue so multiple locals can run concurrently. */
    private static String createResponseQueue(SqsClient sqs, String jobId) {
        String queueName = "textanalysis-local-resp-" + jobId;
        CreateQueueRequest req = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributesWithStrings(java.util.Map.of(
                        "MessageRetentionPeriod", "3600", // 1 hour
                        "VisibilityTimeout", "300",       // 5 minutes
                        "ReceiveMessageWaitTimeSeconds", "20"
                ))
                .build();
        CreateQueueResponse resp = sqs.createQueue(req);
        System.out.println("Created response queue: " + resp.queueUrl());
        return resp.queueUrl();
    }

    private static void deleteQueueQuietly(SqsClient sqs, String queueUrl) {
        try {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            System.out.println("Deleted response queue: " + queueUrl);
        } catch (Exception e) {
            System.err.println("Delete response queue skipped (" + e.getMessage() + ")");
        }
    }
}
