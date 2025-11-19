package worker;

public class WorkerMain {
    public static void main(String[] args) {
        System.out.println("Worker started.");

        // TODO:
        // 1. Receive task from SQS (Manager → Worker queue)
        // 2. Download review text from S3
        // 3. Run NLP analysis (sentiment, category)
        // 4. Upload result to S3
        // 5. Send completion message to Manager
    }
}
