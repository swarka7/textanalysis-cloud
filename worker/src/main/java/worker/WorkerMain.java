package worker;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import edu.stanford.nlp.pipeline.*;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class WorkerMain {

    private static final Region REGION = Region.US_EAST_1;
    // Cap text size per task to avoid OOM on tiny instances
    private static final int MAX_TEXT_CHARS = 500_000;

    private static final String WORKER_TASKS_QUEUE =
            "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-worker-tasks";

    private static final String WORKER_RESULTS_QUEUE =
            "https://sqs.us-east-1.amazonaws.com/501415254993/textanalysis-worker-results";

    private static final String OUTPUT_BUCKET = "textanalysis-output-bucket";

    // Lazy pipelines to minimize RAM: POS-only and full parse
    private static volatile StanfordCoreNLP posPipeline;
    private static volatile StanfordCoreNLP parsePipeline;

    public static void main(String[] args) throws Exception {

        System.out.println("Worker started.");

        SqsClient sqs = SqsClient.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        S3Client s3 = S3Client.builder()
                .region(REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        while (true) {

            ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                    .queueUrl(WORKER_TASKS_QUEUE)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
                    .visibilityTimeout(1200) // give enough time to finish without redelivery
                    .build();

            List<Message> msgs = sqs.receiveMessage(req).messages();
            if (msgs.isEmpty()) {
                continue;
            }

            Message msg = msgs.get(0);
            String body = msg.body();

            Task task = null;
            try {
                if (body.startsWith("TASK#")) {
                    task = parseWorkerTask(body);
                    processTask(task, s3, sqs);
                    // delete only after successful processing (OK/ERROR was sent)
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(WORKER_TASKS_QUEUE)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                } else {
                    System.out.println("Unknown message type, deleting: " + body);
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(WORKER_TASKS_QUEUE)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                }
            } catch (Exception e) {
                e.printStackTrace();
                // Best-effort error report if we parsed a task; leave message if not.
                if (task != null) {
                    sendResultMessage(sqs, task.jobId, task.taskId, task.outputBucket, task.resultKey,
                            "ERROR", e.getMessage());
                    // Delete after reporting the error
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(WORKER_TASKS_QUEUE)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                }
            }
        }
    }

    // ----------------------------------------------
    // Parse worker message
    // ----------------------------------------------

    public static Task parseWorkerTask(String msg) {
        // Format: TASK#jobId#taskId#type#url#outBucket#resultKey
        String[] parts = msg.split("#");
        if (parts.length < 7) {
            throw new IllegalArgumentException("Bad TASK message: " + msg);
        }
        String jobId = parts[1];
        String taskId = parts[2];
        String type = parts[3];
        String url = parts[4];
        String bucket = parts[5];
        String key = parts[6];
        return new Task(jobId, taskId, type, url, bucket, key);
    }

    // ----------------------------------------------
    // Worker Processing
    // ----------------------------------------------

    private static void processTask(Task t, S3Client s3, SqsClient sqs) {
        System.out.println("Processing: job=" + t.jobId + " task=" + t.taskId + " type=" + t.type);

        try {
            // Stream and parse incrementally to reduce RAM on small instances
            String result = runNlpStreaming(t.type, t.url);

            // Use the key provided by manager so summary links line up.
            uploadToS3(s3, result, t.outputBucket, t.resultKey);

            sendResultMessage(sqs, t.jobId, t.taskId, t.outputBucket, t.resultKey, "OK", "");
        } catch (Exception e) {
            e.printStackTrace();
            String detail = (e.getMessage() != null) ? e.getMessage() : e.toString();
            sendResultMessage(sqs, t.jobId, t.taskId, t.outputBucket, t.resultKey, "ERROR", detail);
        }
    }

    // ----------------------------------------------
    // Stream text from remote URL and parse incrementally
    // ----------------------------------------------

    private static String runNlpStreaming(String type, String url) throws IOException {
        StanfordCoreNLP pipe = ("POS".equals(type)) ? getPosPipeline() : getParsePipeline();
        StringBuilder out = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(url).openStream(), StandardCharsets.UTF_8))) {
            String line;
            int totalChars = 0;
            int sentenceIdx = 1;
            while ((line = reader.readLine()) != null && totalChars < MAX_TEXT_CHARS) {
                if (line.isBlank()) continue;
                int remaining = MAX_TEXT_CHARS - totalChars;
                if (line.length() > remaining) {
                    line = line.substring(0, remaining);
                }
                totalChars += line.length();

                CoreDocument doc = new CoreDocument(line);
                pipe.annotate(doc);

                switch (type) {
                    case "POS":
                        doc.tokens().forEach(tok -> out.append(tok.word()).append('/').append(tok.tag()).append(' '));
                        out.append("\n");
                        break;
                    case "CONSTITUENCY":
                        if (doc.sentences().isEmpty()) break;
                        for (var sent : doc.sentences()) {
                            out.append("Sentence ").append(sentenceIdx++).append(":\n");
                            if (sent.constituencyParse() == null) {
                                out.append("NO_PARSE");
                            } else {
                                out.append(sent.constituencyParse().toString());
                            }
                            out.append("\n\n");
                        }
                        break;
                    case "DEPENDENCY":
                        if (doc.sentences().isEmpty()) break;
                        for (var sent : doc.sentences()) {
                            out.append("Sentence ").append(sentenceIdx++).append(":\n");
                            if (sent.dependencyParse() == null) {
                                out.append("NO_PARSE");
                            } else {
                                out.append(sent.dependencyParse().toString());
                            }
                            out.append("\n\n");
                        }
                        break;
                    default:
                        out.append("UNKNOWN TYPE");
                        return out.toString();
                }
            }
        }
        return out.toString().trim();
    }

    // ----------------------------------------------
    // NLP processing
    // ----------------------------------------------

    private static StanfordCoreNLP getPosPipeline() {
        if (posPipeline == null) {
            synchronized (WorkerMain.class) {
                if (posPipeline == null) {
                    Properties props = new Properties();
                    props.setProperty("annotators", "tokenize,ssplit,pos");
                    props.setProperty("ssplit.newlineIsSentenceBreak", "always");
                    props.setProperty("tokenize.options", "normalizeParentheses=false,normalizeOtherBrackets=false");
                    posPipeline = new StanfordCoreNLP(props);
                }
            }
        }
        return posPipeline;
    }

    private static StanfordCoreNLP getParsePipeline() {
        if (parsePipeline == null) {
            synchronized (WorkerMain.class) {
                if (parsePipeline == null) {
                    Properties props = new Properties();
                    // PCFG parser with newline-based sentence splits to handle long docs line by line
                    props.setProperty("annotators", "tokenize,ssplit,pos,parse");
                    props.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
                    props.setProperty("parse.maxlen", "0");
                    props.setProperty("ssplit.newlineIsSentenceBreak", "always");
                    // Use safer tokenizer options; avoid unicodeQuote since CoreNLP 4.5.4 may not support it by name
                    props.setProperty("tokenize.options", "normalizeParentheses=false,normalizeOtherBrackets=false");
                    parsePipeline = new StanfordCoreNLP(props);
                }
            }
        }
        return parsePipeline;
    }

    private static String runNLP(String type, String text) {
        StanfordCoreNLP pipe = ("POS".equals(type)) ? getPosPipeline() : getParsePipeline();
        CoreDocument doc = new CoreDocument(text);
        pipe.annotate(doc);

        switch (type) {
            case "POS":
                return doc.tokens().toString();
            case "CONSTITUENCY":
                if (doc.sentences().isEmpty()) return "NO_SENTENCES";
                StringBuilder constOut = new StringBuilder();
                for (int i = 0; i < doc.sentences().size(); i++) {
                    var sent = doc.sentences().get(i);
                    constOut.append("Sentence ").append(i + 1).append(":\n");
                    if (sent.constituencyParse() == null) {
                        constOut.append("NO_PARSE");
                    } else {
                        constOut.append(sent.constituencyParse().toString());
                    }
                    if (i < doc.sentences().size() - 1) {
                        constOut.append("\n\n");
                    }
                }
                return constOut.toString();
            case "DEPENDENCY":
                if (doc.sentences().isEmpty()) return "NO_SENTENCES";
                StringBuilder depOut = new StringBuilder();
                for (int i = 0; i < doc.sentences().size(); i++) {
                    var sent = doc.sentences().get(i);
                    depOut.append("Sentence ").append(i + 1).append(":\n");
                    if (sent.dependencyParse() == null) {
                        depOut.append("NO_PARSE");
                    } else {
                        depOut.append(sent.dependencyParse().toString());
                    }
                    if (i < doc.sentences().size() - 1) {
                        depOut.append("\n\n");
                    }
                }
                return depOut.toString();
            default:
                return "UNKNOWN TYPE";
        }
    }

    // ----------------------------------------------
    // Upload result to S3
    // ----------------------------------------------

    private static void uploadToS3(S3Client s3, String content, String bucket, String key) throws IOException {
        Path temp = Files.createTempFile("worker-", ".txt");
        Files.writeString(temp, content);

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        s3.putObject(req, temp);
    }

    // ----------------------------------------------
    // Send result back to manager
    // ----------------------------------------------
    
    private static void sendResultMessage(SqsClient sqs,
                                          String jobId,
                                          String taskId,
                                          String bucket,
                                          String key,
                                          String status,
                                          String detail) {
        String body = String.join("#",
                "RESULT",
                jobId,
                taskId,
                bucket,
                key,
                status,
                detail == null ? "" : detail);

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(WORKER_RESULTS_QUEUE)
                .messageBody(body)
                .build());

        System.out.println("Sent result for job " + jobId + " task " + taskId + " (" + status + ")");
    }
}
