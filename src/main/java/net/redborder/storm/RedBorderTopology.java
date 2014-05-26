package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import java.io.FileNotFoundException;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.util.ConfigData;
import net.redborder.storm.util.KafkaConfigFile;
import storm.trident.TridentTopology;

public class RedBorderTopology {

    static ConfigData config;
    static KafkaConfigFile kafkaConfig;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException, InterruptedException {

        String topologyName = "redBorder-Topology";

        if (args.length != 1) {

            System.out.println("./storm jar {name_jar} {main_class} {local|cluster}");

        } else {
            kafkaConfig = new KafkaConfigFile();
            config = new ConfigData(kafkaConfig);

            TridentTopology topology = topology();

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = config.getConfig(args[0]);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, conf, topology.build());

                for (int i = 0; i < 10; i++) {
                    Thread.sleep(1000);
                }

                //Utils.sleep(1000000);
                //cluster.killTopology(topologyName);
                //cluster.shutdown();
            } else if (args[0].equalsIgnoreCase("cluster")) {
                Config conf = config.getConfig(args[0]);
                StormSubmitter.submitTopology(topologyName, conf, topology.build());
                System.out.println("Topology: " + topologyName + " uploaded successfully.");
            }
        }
    }

    public static TridentTopology topology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        //int flowPartition = config.getKafkaPartitions("rb_flow");
        topology.newStream("rb_flow", new TridentKafkaSpout(kafkaConfig, "traffics").builder())
                .parallelismHint(4)
                .shuffle();
                //.each(new Fields("str"), new ProducerKafkaFunction(kafkaConfig, "rb_flow_pre"), new Fields("producer"))
                //.parallelismHint(2);

        return topology;
    }

}
