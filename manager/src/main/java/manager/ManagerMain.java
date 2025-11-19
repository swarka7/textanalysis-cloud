package manager;

public class ManagerMain {
    public static void main(String[] args) {
        System.out.println("Manager started.");

        // TODO:
        // 1. Listen to SQS (LocalApp → Manager queue)
        // 2. Create Worker EC2s if needed
        // 3. Split input into tasks
        // 4. Send tasks to Workers (SQS)
        // 5. Collect Worker results
        // 6. Upload summary file to S3
        // 7. Notify LocalApp
    }
}
