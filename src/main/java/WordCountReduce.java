import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.pcj.PCJ;
import org.pcj.RegisterStorage;
import org.pcj.StartPoint;
import org.pcj.Storage;

@RegisterStorage(WordCountReduce.Shareable.class)
public class WordCountReduce implements StartPoint {
    public static void main(String[] args) throws IOException{
        PCJ.executionBuilder(WordCountReduce.class)
                .addNodes(new File(args.length>0 ? args[0] : "nodes.txt"))
                .start();
    }

    @Storage(WordCountReduce.class)
    enum Shareable {
        localCounts
    }

    private Map<String, Long> localCounts;
    private String myFileName;

    public void main() throws IOException {
        readConfiguration();
        PCJ.barrier();

        long start = System.nanoTime();
        Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
        localCounts = Files.readAllLines(Paths.get(myFileName),
                StandardCharsets.ISO_8859_1) //StandardCharsets.UTF_8
                .stream()
                .map(WORD_BOUNDARY::split)
                .flatMap(Arrays::stream)
                .filter(word -> !word.isEmpty())
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        HashMap::new,
                        Collectors.counting()));

        long intermediate = System.nanoTime();
        PCJ.barrier();

        if (PCJ.myId() == 0) {
            Map<String, Long> resultMap = PCJ.reduce(
                    (mine, their) -> Stream.concat(mine.entrySet().stream(), their.entrySet().stream())
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue,
                                    Long::sum
                            )), Shareable.localCounts);
            long stop = System.nanoTime();
            System.out.println("threads\ttotal\tcounting\treduction");
            System.out.println(PCJ.threadCount() + "\t" + (stop - start) * 1e-9 + "\t" + (intermediate - start) * 1e-9 + "\t" + (stop - intermediate) * 1e-9);

            resultsToFile("counts.txt", resultMap);
        }
    }

    private void readConfiguration() throws IOException {
        List<String> configurationFileLines = Files.readAllLines(Paths.get("wordcount.config"));
        myFileName = configurationFileLines.get(PCJ.myId());
    }

    private void resultsToFile(String fileName, Map<String, Long> resultMap) throws IOException {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(Paths.get(fileName)))) {
            resultMap.forEach((k, v) -> { out.println(k + "\t" + v); });
        }
    }
}
