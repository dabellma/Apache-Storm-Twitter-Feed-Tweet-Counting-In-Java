import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<>(50000);
    private TwitterStream twitterStream;


    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onException(Exception ex) {}

            @Override
            public void onDeletionNotice(
                    StatusDeletionNotice statusDeletionNotice) {}

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {}

            @Override
            public void onStallWarning(StallWarning warning) {}

        };

        twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build()).getInstance();

        twitterStream.setOAuthConsumer("enter oauth consumer info here", "enter secondary oauth consumer info here");
        twitterStream.setOAuthAccessToken(new AccessToken("enter oauth access token info here", "enter secondary oauth access token info here"));
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status status = queue.poll();

        if (status == null) {
            Utils.sleep(10);
        } else {
            for (HashtagEntity e : status.getHashtagEntities()) {
                collector.emit(new Values(e.getText()));
            }
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("tweets")); }
}
