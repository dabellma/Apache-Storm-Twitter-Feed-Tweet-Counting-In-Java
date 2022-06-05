import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ReportBolt extends BaseRichBolt {
    private ConcurrentHashMap<String, Integer> counts;
    private int onlyTop100;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.counts = new ConcurrentHashMap<>();
        this.onlyTop100 = 0;
    }

    public void execute(Tuple tuple) { //these if statements ensure all tuples besides the tick tuple come through first
        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {

        } else if (tuple.getStringByField("tweet").equals("ticktuple")) {
            System.out.println("tick tuple in report bolt ");
            HashMap<String, Integer> sortedCounts = sortMap(this.counts);

            try {
//                FileWriter fileWriter = new FileWriter("C:\\Users\\Marky\\Documents\\College Schoolwork\\outputfromintellij4.txt", true);
                FileWriter fileWriter = new FileWriter("/s/chopin/a/grad/dabellma/CS535/PA2/outputforlossycount.txt", true);

                fileWriter.write(String.valueOf(Instant.now()) + ": ");
                if (sortedCounts.size() > 100) {
                    for (Map.Entry<String, Integer> eachEntry : sortedCounts.entrySet()) {
                        if (onlyTop100 < 100) {
                            fileWriter.write("<" + eachEntry.getKey() + "> ");
                            onlyTop100 += 1;
                        }
                    }

                    onlyTop100 = 0;
                } else {
                    for (Map.Entry<String, Integer> eachEntry : sortedCounts.entrySet()) {
                        fileWriter.write("<" + eachEntry.getKey() + "> ");
                    }
                }

                fileWriter.write(System.getProperty("line.separator"));

                this.counts = new ConcurrentHashMap<>();
                fileWriter.close();
                System.out.println("Successfully wrote to the file");
            } catch (IOException ioException) {
                System.out.println("An ioException occurred.");
                ioException.printStackTrace();
            }
        } else {
            String word = tuple.getStringByField("tweet");
            Integer count = tuple.getIntegerByField("count");
            System.out.println("putting tuple in map in report bolt");
            this.counts.put(word, count);
        }

    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }

    private HashMap<String, Integer> sortMap(Map<String, Integer> mapToSort) {
        List<Map.Entry<String, Integer>> list = new LinkedList<>(mapToSort.entrySet());
        Collections.sort(list, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));

        HashMap<String, Integer> temp = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> aa: list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
