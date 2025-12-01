package worker;

public class Task {
    public final String jobId;
    public final String taskId;
    public final String type;
    public final String url;
    public final String outputBucket;
    public final String resultKey;

    public Task(String jobId, String taskId, String type, String url, String outputBucket, String resultKey) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.type = type;
        this.url = url;
        this.outputBucket = outputBucket;
        this.resultKey = resultKey;
    }
}
