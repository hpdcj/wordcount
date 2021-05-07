import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.pcj.Group;
import org.pcj.NodesDescription;
import org.pcj.PCJ;
import org.pcj.RegisterStorage;
import org.pcj.StartPoint;
import org.pcj.Storage;

@RegisterStorage(WordCountPcj.Shared.class)
public class WordCountPcj implements StartPoint {

    private void resultsToFile(String fileName) throws FileNotFoundException, IOException {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(Paths.get(fileName)))) {
            this.resultMap.forEach((k, v) -> {
                out.println(k + "\t" + v);
            });
        }
    }

    @Storage(WordCountPcj.class)
    enum Shared {

        reducing,
        localCounts
    }
    public HashMap<String, Integer> localCounts;
    public HashMap<String, Integer> reducing;

    public static void main(String[] args) throws IOException {
        String nodeFileName = "nodes.txt";
        if (args.length > 0) {
            nodeFileName = args[0];
        }
        PCJ.start(WordCountPcj.class, new NodesDescription(nodeFileName));
    }
    String myFileName = "";
    long start, intermediate, stop;

    @Override
    public void main() throws Throwable {
        Runnable reduceFunction = this::getGlobalCountSerial;
        readConfiguration();
        PCJ.barrier();
        start = System.nanoTime();
        countWords();
        intermediate = System.nanoTime();
        PCJ.barrier();
        reduceFunction.run();
        if (PCJ.myId() == 0) {
            stop = System.nanoTime();
            System.out.println("threads\ttotal\tcounting\treduction");
            System.out.println(PCJ.threadCount() + "\t" + (stop - start) * 1e-9 + "\t" + (intermediate - start) * 1e-9 + "\t" + (stop - intermediate) * 1e-9);
            resultsToFile("counts.txt");
        }
    }

    private void readConfiguration() throws IOException {
        List<String> configurationFileLines = Files.readAllLines(Paths.get("wordcount.config"));
        myFileName = configurationFileLines.get(PCJ.myId());

    }

    private void countWords() throws IOException {
        final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
        final Map<String, Integer> wordCounts = new HashMap<>();

//        Files.readAllLines(Paths.get(myFileName), StandardCharsets.UTF_8).stream().forEach(line -> {
        Files.readAllLines(Paths.get(myFileName), StandardCharsets.ISO_8859_1).stream().forEach(line -> {
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                } else {
                    wordCounts.merge(word, 1, Integer::sum);
                }
            }
        });
        PCJ.putLocal(wordCounts, Shared.localCounts);
    }

    Map<String, Integer> resultMap;

    private void iterateAndJoinMapWithRemoteMaps(Map<String, Integer> resultMap, int init, int stop, int add) {
        for (int i = init; i < stop; i += add) {
            Map<String, Integer> remoteMap = PCJ.get(i, Shared.localCounts);
            remoteMap.forEach((k, v) -> resultMap.merge(k, v, Integer::sum));
        }
    }

    private void getGlobalCountSerial() {
        if (PCJ.myId() == 0) {
            resultMap = new HashMap<>();
            iterateAndJoinMapWithRemoteMaps(resultMap, 0, PCJ.threadCount(), 1);
        }
    }

    private Group tryJoiningNode0Group() {
        int localThreadCount = Runtime.getRuntime().availableProcessors();
        Group localGroup = null;
        if (PCJ.myId() < localThreadCount) {
            localGroup = PCJ.join("localGroup");
        }
        return localGroup;
    }

    private void getGlobalCountParallelNode0() {
        final Group localGroup = tryJoiningNode0Group();
        if (localGroup != null) {
            int localThreadCount = localGroup.threadCount();
            resultMap = new HashMap<>();
            iterateAndJoinMapWithRemoteMaps(resultMap, localGroup.myId(), PCJ.threadCount(), localThreadCount);
            PCJ.putLocal(resultMap, Shared.localCounts);

            localGroup.asyncBarrier().get();

            if (PCJ.myId() == 0) {
                resultMap = new HashMap<>();
                iterateAndJoinMapWithRemoteMaps(resultMap, 0, localGroup.threadCount(), 1);
            }
        }

    }

    private void getGlobalCountReduceLocalFirst() {
        int localThreadCount = Runtime.getRuntime().availableProcessors();
        int minId = PCJ.myId() / localThreadCount * localThreadCount;
        int maxId = Math.min(minId + localThreadCount, PCJ.threadCount());
        resultMap = new HashMap<>();
        if (PCJ.myId() == minId) {
            iterateAndJoinMapWithRemoteMaps(resultMap, minId, maxId, 1);
            PCJ.putLocal(resultMap, Shared.localCounts);
        }
        PCJ.barrier();
        if (PCJ.myId() == 0) {
            resultMap = new HashMap<>();
            iterateAndJoinMapWithRemoteMaps(resultMap, 0, PCJ.threadCount(), localThreadCount);
        }
    }

    private void getGlobalCountHypercube() {
        resultMap = PCJ.getLocal(Shared.localCounts);
        int mask = 0;
        int d = Integer.numberOfTrailingZeros(PCJ.threadCount());
        int myId = PCJ.myId();
        PCJ.barrier();
        for (int i = 0; i < d; i++) {
            if ((PCJ.myId() & mask) == 0) {
                if ((myId & (1 << i)) != 0) {
                    int dest = myId ^ (1 << i);
                    PCJ.put(resultMap, dest, Shared.reducing);
                    PCJ.barrier(dest);
                } else {
                    int src = myId ^ (1 << i);
                    PCJ.barrier(src);
                    HashMap<String, Integer> reducedArgument = PCJ.getLocal(Shared.reducing);
                    reducedArgument.forEach((k, v) -> resultMap.merge(k, v, Integer::sum));
                }
            }
            mask ^= (1 << i);
            PCJ.barrier();
        }
    }
}
