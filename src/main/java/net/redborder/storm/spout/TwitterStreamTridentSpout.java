/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.trident.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.Lists;
import net.redborder.storm.spout.TwitterStreamSpout;
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
import storm.trident.spout.IBatchSpout;

/**
 *
 * @author andresgomez
 */
public class TwitterStreamTridentSpout implements IBatchSpout {

    private static final Logger LOGGER = Logger.getLogger(TwitterStreamSpout.class.getName());
    private SpoutOutputCollector collector;
    private Client hbc;
    private final BlockingQueue<String> tweetsToProcess = new LinkedBlockingQueue<String>();
    private static List<String> TERMS = Lists.newArrayList();
    private static final String QUERY = "twitter.query";
    private static final Pattern TWEET_ID_PATTERN = Pattern.compile("\"id_str\"\\s*:\\s*\"(\\d+)\"");

    private static final String CONSUMER_KEY = "twitter.consumerKey";
    private static final String CONSUMER_SECRET = "twitter.consumerSecret";
    private static final String TOKEN = "twitter.token";
    private static final String TOKEN_SECRET = "twitter.tokenSecret";

    @Override
    public void open(Map stormConf, TopologyContext context) {
        checkNotNull(stormConf.get(CONSUMER_KEY), "'consumerKey' config not set");
        checkNotNull(stormConf.get(CONSUMER_SECRET), "'consumerSecret' config not set");
        checkNotNull(stormConf.get(TOKEN), "'token' config not set");
        checkNotNull(stormConf.get(TOKEN_SECRET), "'tokenSecret' config not set");
        checkNotNull(stormConf.get(QUERY), "'query' not set");

        LOGGER.info("Configuring environment for spout: " + context.getThisComponentId() + "-" + context.getThisTaskId());
        this.collector = collector;

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        String query = stormConf.get(QUERY).toString();
        if (!query.equals("none")) {
            if (query.contains("; ")) {
                query = query.replace("; ", ",");
            }

            TERMS = Lists.newArrayList(query);
            endpoint.trackTerms(TERMS);
        }
        Authentication hosebirdAuth = new OAuth1((String) stormConf.get(CONSUMER_KEY),
                (String) stormConf.get(CONSUMER_SECRET),
                (String) stormConf.get(TOKEN),
                (String) stormConf.get(TOKEN_SECRET));

        ClientBuilder builder = new ClientBuilder()
                .name("twitter-spout")
                .hosts(hosebirdHosts)
                .endpoint(endpoint)
                .authentication(hosebirdAuth)
                .processor(new StringDelimitedProcessor(tweetsToProcess));

        hbc = builder.build();
        hbc.connect();    }

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
