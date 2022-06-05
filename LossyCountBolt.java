import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LossyCountBolt extends BaseRichBolt {
    private ConcurrentHashMap<String, EntryInLossyCount> entries = new ConcurrentHashMap<>();
    private List<String> toRemove = new ArrayList<>();

    private OutputCollector collector;
    private int currentBucketNumber;
    private static double epsilon = 0.01;
    private static double threshold = 0.01;
    private int bucketSize;
    private int totalCount;
    private int error;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.currentBucketNumber = 1;
        this.bucketSize = (int) (1 / epsilon);
        this.totalCount = 0;
        this.error = 0;
    }

    public void execute(Tuple tuple) {

        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            for (Map.Entry<String, EntryInLossyCount> entry : entries.entrySet()) {
                if (entry.getValue().getFrequency() > ((threshold - epsilon) * totalCount)) {
                    collector.emit(new Values(entry.getKey(), entry.getValue().getFrequency()));
                }
            }

            collector.emit(new Values("ticktuple", 999));
            entries = new ConcurrentHashMap<>();
            currentBucketNumber = 1;
            totalCount = 0;
            error = 0;


        } else {
            runLossyCountAlgorithm(tuple);
        }
    }

    public void runLossyCountAlgorithm(Tuple tuple) {
        String tweet = tuple.getStringByField("tweets");
        EntryInLossyCount entryInLossyCount = entries.get(tweet);

        if (entryInLossyCount == null) {
            entries.put(tweet, new EntryInLossyCount(tweet, 1, error));
        } else  {
            int currentFrequency = entryInLossyCount.getFrequency();
            entryInLossyCount.setFrequency(currentFrequency + 1);
        }

        totalCount++;

        if (totalCount % bucketSize == 0) {
            for (Map.Entry<String, EntryInLossyCount> eachEntry : entries.entrySet()) {
                if ((eachEntry.getValue().getFrequency() + eachEntry.getValue().getError()) <= currentBucketNumber) {
                    toRemove.add(eachEntry.getKey());
                }
            }

            //removing now to avoid concurrent modification exception
            for (String tweetToRemove : toRemove) {
                entries.remove(tweetToRemove);
            }

            //resetting list for next iteration of removals after a bucket is processed
            toRemove = new ArrayList<>();

            error++;
            currentBucketNumber++;
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("tweet", "count"));}
}
