package com.redborder.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.metamx.tranquility.storm.BeamBolt;
import com.metamx.tranquility.storm.TridentBeamState;
import com.metamx.tranquility.storm.TridentBeamStateFactory;
import com.metamx.tranquility.storm.TridentBeamStateUpdater;
import com.redborder.storm.trident.function.EventBuilderFunction;
import com.redborder.storm.trident.spout.TrindetKafkaSpout;
import com.redborder.storm.trident.spout.TwitterStreamTridentSpout;
import com.redborder.storm.trident.state.MemcachedMultipleState;
import com.redborder.storm.trident.state.MemcachedMultipleState.Options;
import com.redborder.storm.trident.state.MemcachedState;
import com.redborder.storm.trident.state.query.twitterQuery;
import com.redborder.storm.trident.topologies.TridentRedBorderTopologies;
import com.redborder.storm.trident.updater.twitterUpdater;
import com.redborder.storm.util.CreateConfig;
import com.redborder.storm.util.GetKafkaConfig;
import com.redborder.storm.util.KeyUtils;
import com.redborder.storm.util.RBEventType;
import com.redborder.storm.util.druid.MyBeamFactoryMapEvent;
import com.redborder.storm.util.druid.MyBeamFactoryMapFlow;
import com.redborder.storm.util.druid.MyBeamFactoryMapMonitor;
import com.redborder.storm.util.state.ConcatKeyBuilder;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.spy.memcached.MemcachedClient;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

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
            for (Object o : list) {
                System.out.println("\n\n\n" + _str + " " + o.toString());
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
            if (id != null) {
                collector.emit(new Values(id));
            } else {
                collector.emit(new Values("-"));
            }
        }

    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {

        String topologyName = "redBorder-Topology";
        if (args.length != 1) {
            System.out.println("./storm jar {name_jar} {main_class} {local|cluster}");
        } else {
            TridentRedBorderTopologies topologies = new TridentRedBorderTopologies();

             int PORT = 52030;
             
             TridentTopology topology = topologies.flowMSEtopology();         

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = new CreateConfig(args[0]).makeConfig();

                LocalCluster cluster = new LocalCluster();
                startLocalMemcacheInstance(PORT);
                cluster.submitTopology(topologyName, conf, topology.build());

                //Utils.sleep(1000000);
                //cluster.killTopology(topologyName);
                //cluster.shutdown();

            } else if (args[0].equalsIgnoreCase("cluster")) {

                Config conf = new CreateConfig(args[0]).makeConfig();
                StormSubmitter.submitTopology(topologyName, conf, topology.build());
                System.out.println("Topology: " + topologyName  + " uploaded successfully.");
            }
        }
    }
}
