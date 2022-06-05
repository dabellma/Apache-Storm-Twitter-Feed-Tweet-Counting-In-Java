import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class LossyCountTopology {

    private static final String TWITTER_SPOUT_ID = "twitter-spout";
    private static final String LOSSY_COUNT_BOLT_ID = "lossy-count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String REPORT_BOLT_PARALLEL_ID = "report-bolt-parallel";
    private static final String TOPOLOGY_NAME = "lossy-count-topology";

    public static void main(String[] args) throws Exception {

        TwitterSpout spout = new TwitterSpout();
        LossyCountBolt lossyCountBolt = new LossyCountBolt();
        ReportBolt reportBolt = new ReportBolt();
        ReportBoltParallel reportBoltParallel = new ReportBoltParallel();

        TopologyBuilder nonParallelBuilder = new TopologyBuilder();
        TopologyBuilder parallelBuilder = new TopologyBuilder();

        nonParallelBuilder.setSpout(TWITTER_SPOUT_ID, spout);
        //TwitterBucketSpout --> LossyCountBolt
        nonParallelBuilder.setBolt(LOSSY_COUNT_BOLT_ID, lossyCountBolt).allGrouping(TWITTER_SPOUT_ID);
        //LossyCountBolt --> ReportBolt
        nonParallelBuilder.setBolt(REPORT_BOLT_ID, reportBolt).allGrouping(LOSSY_COUNT_BOLT_ID);


        parallelBuilder.setSpout(TWITTER_SPOUT_ID, spout);
        //TwitterBucketSpout --> LossyCountBolt
        parallelBuilder.setBolt(LOSSY_COUNT_BOLT_ID, lossyCountBolt, 4).setNumTasks(8).fieldsGrouping(TWITTER_SPOUT_ID, new Fields("tweets"));
        //LossyCountBolt --> ReportBolt
        parallelBuilder.setBolt(REPORT_BOLT_PARALLEL_ID, reportBoltParallel).globalGrouping(LOSSY_COUNT_BOLT_ID);



        Config nonParallelConfig = new Config();
        Config parallelConfig = new Config();
        parallelConfig.setNumWorkers(4);


        StormSubmitter.submitTopology(TOPOLOGY_NAME, nonParallelConfig, nonParallelBuilder.createTopology());
//        StormSubmitter.submitTopology(TOPOLOGY_NAME, parallelConfig, parallelBuilder.createTopology());

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology(TOPOLOGY_NAME, nonParallelConfig, nonParallelBuilder.createTopology());
//        cluster.submitTopology(TOPOLOGY_NAME, parallelConfig, parallelBuilder.createTopology());
//        Thread.sleep(10000);

    }
}
