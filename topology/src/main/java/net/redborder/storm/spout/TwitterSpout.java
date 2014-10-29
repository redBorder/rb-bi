package net.redborder.storm.spout;

import storm.trident.spout.IBatchSpout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storm.trident.operation.TridentCollector;

/**
 * @author andresgomez
 */
public class TwitterSpout implements IBatchSpout {

    private static final Logger LOGGER = Logger.getLogger(TwitterSpout.class.getName());
    private SpoutOutputCollector collector;
    private Client hbc;
    private final BlockingQueue<String> tweetsToProcess = new LinkedBlockingQueue<String>();
    private static List<String> TERMS = Lists.newArrayList();
    private String QUERY = "none";
    private static final Pattern TWEET_ID_PATTERN = Pattern.compile("\"id_str\"\\s*:\\s*\"(\\d+)\"");

    private String CONSUMER_KEY = null;
    private String CONSUMER_SECRET = null;
    private String TOKEN = null;
    private String TOKEN_SECRET = null;

    public TwitterSpout(String consumerKey, String consumerSecret, String token, String tokenSecret) {
        this.CONSUMER_KEY = consumerKey;
        this.CONSUMER_SECRET = consumerSecret;
        this.TOKEN = token;
        this.TOKEN_SECRET = tokenSecret;
    }

    public TwitterSpout(String consumerKey, String consumerSecret, String token, String tokenSecret, String query) {
        this(consumerKey, consumerSecret, token, tokenSecret);
        this.QUERY = query;
    }

    @Override
    public void open(Map stormConf, TopologyContext context) {


        LOGGER.info("Configuring environment for spout: " + context.getThisComponentId() + "-" + context.getThisTaskId());
        this.collector = collector;

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        String query = QUERY.toString();

        if (!query.equals("none")) {
            if (query.contains("; ")) {
                query = query.replace("; ", ",");
            }

            TERMS = Lists.newArrayList(query);
            endpoint.trackTerms(TERMS);
        }
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY,
                CONSUMER_SECRET,
                TOKEN,
                TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("twitter-spout")
                .hosts(hosebirdHosts)
                .endpoint(endpoint)
                .authentication(hosebirdAuth)
                .processor(new StringDelimitedProcessor(tweetsToProcess));

        hbc = builder.build();
        hbc.connect();
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        try {
            String tweet = tweetsToProcess.take();
            String tweetId = null;
            Matcher m = TWEET_ID_PATTERN.matcher(tweet);
            if (m.find()) {
                tweetId = m.group(1);
            }
            collector.emit(new Values(tweet));
        } catch (InterruptedException e) {
            collector.reportError(e);
        }
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
        hbc.stop();
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tweet");
    }

}