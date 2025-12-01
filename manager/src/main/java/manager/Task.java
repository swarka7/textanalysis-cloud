package manager;

public class Task {
    public final int lineNumber;
    public final String type;
    public final String url;

    public Task(int lineNumber, String type, String url) {
        this.lineNumber = lineNumber;
        this.type = type;
        this.url = url;
    }
}
