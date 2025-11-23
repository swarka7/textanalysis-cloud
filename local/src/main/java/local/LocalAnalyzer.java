package local;

import edu.stanford.nlp.pipeline.*;
import java.util.*;

public class LocalAnalyzer {

    private static StanfordCoreNLP pipeline;

    // Initialize NLP pipeline once
    static {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,sentiment");
        pipeline = new StanfordCoreNLP(props);
    }

    public static String analyzeText(String text) {
        CoreDocument document = new CoreDocument(text);
        pipeline.annotate(document);

        StringBuilder sb = new StringBuilder();

        for (CoreSentence sentence : document.sentences()) {
            String sent = sentence.text();
            String sentiment = sentence.sentiment();

            sb.append("Sentence: ").append(sent).append("\n");
            sb.append("Sentiment: ").append(sentiment).append("\n\n");
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        String text = "Stanford CoreNLP is a great tool for natural language processing. "
                + "I love using it for text analysis. "
                + "However, sometimes it can be a bit slow.";
        System.out.println(analyzeText(text));
    }
}
