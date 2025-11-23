package local;

import edu.stanford.nlp.pipeline.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class LocalAnalyzer {

    private static final StanfordCoreNLP pipeline;

    static {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,parse");
        pipeline = new StanfordCoreNLP(props);
    }

    public static void main(String[] args) throws Exception {

        // Choose analysis type manually for now
        String analysisType = "DEPENDENCY"; // POS | CONSTITUENCY | DEPENDENCY

        String text = Files.readString(Path.of("local/src/main/resources/sample.txt"));


        System.out.println("Input:\n" + text + "\n");

        CoreDocument doc = new CoreDocument(text);
        pipeline.annotate(doc);

        switch (analysisType) {
            case "POS":
                doc.tokens().forEach(t ->
                        System.out.println(t.word() + "\t" + t.tag()));
                break;

            case "CONSTITUENCY":
                doc.sentences().forEach(s ->
                        System.out.println(s.constituencyParse()));
                break;

            case "DEPENDENCY":
                doc.sentences().forEach(s ->
                        System.out.println(s.dependencyParse()));
                break;
        }
    }
}
