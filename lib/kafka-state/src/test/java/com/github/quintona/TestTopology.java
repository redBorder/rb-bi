package com.github.quintona;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.util.ByteArrayBuffer;

import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.ZkHosts;

public class TestTopology {

    public static class AppendFunction extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String text = new String(tuple.getBinary(0));
            collector.emit(new Values(text + "_end!"));
        }
    }

    public static TridentTopology makeTopology() throws IOException {
        TridentTopology topology = new TridentTopology();

        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(new ZkHosts("localhost"), "test", "groupTest");

        topology.newStream("kafka",
                new TransactionalTridentKafkaSpout(spoutConfig))
                .each(new Fields("bytes"), new AppendFunction(), new Fields("text"))
                .partitionPersist(KafkaState.nonTransactional("localhost", "test1", new KafkaState.Options()), new Fields("text"), new KafkaStateUpdater("text"));

        return topology;
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter
                    .submitTopology(args[0], conf, makeTopology().build());
        } else {
            conf.setMaxTaskParallelism(3);
            conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(new String[]{"127.0.0.1"}));
            conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            conf.put(Config.STORM_ZOOKEEPER_ROOT, "/storm");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test-kafka-push", conf,
                    makeTopology().build());

            Thread.sleep(100000);

            cluster.shutdown();
        }
    }
}
