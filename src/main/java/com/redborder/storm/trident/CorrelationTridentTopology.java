package com.redborder.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.Utils;
import storm.trident.operation.BaseFunction;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.redborder.storm.trident.bolt.EventBuilderTrindetFuction;
import com.redborder.storm.trident.spout.TrindetKafkaSpout;
import com.redborder.storm.trident.spout.TwitterStreamTridentSpout;
import com.redborder.storm.trident.state.MemcachedState;
import com.redborder.storm.util.GetKafkaConfig;
import com.redborder.storm.util.KeyUtils;
import com.redborder.storm.util.RBEventType;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import java.io.FileNotFoundException;
import java.util.Map;
import net.spy.memcached.MemcachedClient;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;

public class CorrelationTridentTopology {

    private static final MemCacheDaemon<LocalCacheElement> daemon
            = new MemCacheDaemon<LocalCacheElement>();

    private static void startLocalMemcacheInstance(int port) {
        System.out.println("Starting local memcache");
        CacheStorage<Key, LocalCacheElement> storage
                = ConcurrentLinkedHashMap.create(
                        ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 100, 1024 * 500);
        daemon.setCache(new CacheImpl(storage));
        daemon.setAddr(new InetSocketAddress("localhost", port));
        daemon.start();
    }

    public static class PrinterBolt extends BaseFunction {

        String _str = "";

        public PrinterBolt(String str) {
            _str = str;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            List<Object> list = tuple.getValues();
            for (Object o : list){     
            System.out.println(_str + " " + o.toString());
            }

        }

    }

    public static class GetSensor extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            
            Map<String, Object> event = (Map<String, Object>) tuple.getValueByField("event");
            collector.emit(new Values(event.get("sensor_name")));
        }

    }

    public static class memcachedUpdate extends BaseStateUpdater<MapState<Map<String, Object>>> {

        String _key;

        public memcachedUpdate(String key) {
            _key = key;
        }

        public void updateState(MapState<Map<String, Object>> state, List<TridentTuple> tuples, TridentCollector collector) {
            List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
            for (TridentTuple t : tuples) {
                events.add((Map<String, Object>) t.getValue(0));
            }
            state.multiPut(KeyUtils.toKeys(_key), events);
        }
    }

    public static class queryUpdate extends BaseQueryFunction<MapState<Map<String, Object>>, Map<String, Object>> {

        String _key;

        public queryUpdate(String key) {
            _key = key;
        }

        @Override
        public void execute(TridentTuple tuple, Map<String, Object> result, TridentCollector collector) {
            if (result != null) {
                collector.emit(new Values(result));
            }
        }

        @Override
        public List<Map<String, Object>> batchRetrieve(MapState<Map<String, Object>> state, List<TridentTuple> tuples) {
            List<Map<String, Object>> labels = new ArrayList<Map<String, Object>>();

            List<Map<String, Object>> memcached = state.multiGet(KeyUtils.toKeys(_key));
            if (memcached != null && !memcached.isEmpty()) {
                Map<String, Object> event = memcached.get(0);
                if (event == null) {
                    for (int i = 0; i < tuples.size(); i++) {
                        labels.add(null);
                    }
                } else {

                    for (TridentTuple tuple : tuples) {
                        labels.add(event);
                    }
                }
            } else {
                for (int i = 0; i < tuples.size(); i++) {
                    labels.add(null);
                }
            }

            return labels;
        }
    }

    public static class GetTweetID extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Map<String, Object> tweet = (Map<String, Object>) tuple.getValueByField("tweetMap");
            Map<String, Object> user = (Map<String, Object>) tweet.get("user");
            String id = String.valueOf(user.get("id"));
            collector.emit(new Values(id));

        }

    }

    public static class GetID extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
            String id = String.valueOf(event.get("userid"));
            collector.emit(new Values(id));
        }

    }

    public static class Correlation extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Map<String, Object> event = (Map<String, Object>) tuple.getValueByField("cacheEvent");
            Map<String, Object> tweet = (Map<String, Object>) tuple.getValueByField("tweetMap");
            System.out.println(event.toString());
            System.out.println(tweet.toString());
        }

    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {

        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();

        zkConfig.setTopicInt(RBEventType.MONITOR);

        int PORT = 52030;

        StateFactory memcached = MemcachedState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)));
        //StateFactory memcached1 = MemcachedState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)));

        
        TridentState eventState = topology.newStream("rb_monitor", new TrindetKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new EventBuilderTrindetFuction(RBEventType.MONITOR), new Fields("topic", "event"))
                //.each(new Fields("event"), new GetID(), new Fields("id"))
                .partitionPersist(memcached, new Fields("event"), new memcachedUpdate("rb_event"), new Fields("cacheEvent"));

        Stream tweetState = topology.newStream("twitterStream", new TwitterStreamTridentSpout())
                .each(new Fields("tweet"), new EventBuilderTrindetFuction(5), new Fields("topic", "tweetMap"))
                .project(new Fields("tweetMap"));
                //.each(new Fields("tweetMap", "tweet"), new GetTweetID(), new Fields("userId"))
                //.stateQuery(state, new queryUpdate(), new Fields("event"));
                //.stateQuery(state, new MapGet(), new Fields("event"));
               // .partitionPersist(memcached1, new Fields("tweetMap"), new memcachedUpdate("tweet"), new Fields("cacheTweet"));

        topology.merge(new Fields("tweet"), tweetState)
                .stateQuery(eventState, new queryUpdate("rb_event"), new Fields("event"))
                .each(new Fields("tweet", "event"), new PrinterBolt("tweeeee:"), new Fields("a"));

        if (args[0].equalsIgnoreCase("local")) {
            Config conf = new Config();
            conf.setMaxTaskParallelism(1);
            conf.setDebug(false);
            conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 10000);

            String CONSUMER_KEY = "twitter.consumerKey";
            String CONSUMER_SECRET = "twitter.consumerSecret";
            String TOKEN = "twitter.token";
            String TOKEN_SECRET = "twitter.tokenSecret";
            String QUERY = "twitter.query";

            conf.put(CONSUMER_KEY, "Vkoyw2Bwgk13RFaTyJlYQ");
            conf.put(CONSUMER_SECRET, "TkW74gdR764dH6lOkD3cKSwGLMKy7xrA9s7ZCZsqRno");
            conf.put(TOKEN, "154536310-Yxg7DqA6mg982MSxG2peKa6TIUf00loFJnVMwOaP");
            conf.put(TOKEN_SECRET, "oG5JIcg1CKCDNQwqIVrt1RVR2bqPWZ91DUJXEYefnjCkX");
            conf.put(QUERY, "redborder");

            LocalCluster cluster = new LocalCluster();
            startLocalMemcacheInstance(PORT);
            cluster.submitTopology("Redborder-Topology", conf, topology.build());

            Utils.sleep(100000);
            cluster.killTopology("Redborder-Topology");
            cluster.shutdown();

        } else if (args[0].equalsIgnoreCase("cluster")) {

            Config conf = new Config();
            StormSubmitter.submitTopology("Redborder-Topology", conf, topology.build());
        }
    }
}
