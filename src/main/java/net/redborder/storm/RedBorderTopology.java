package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import com.metamx.tranquility.storm.TridentBeamStateFactory;
import com.metamx.tranquility.storm.TridentBeamStateUpdater;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import net.redborder.storm.function.*;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.state.*;
import net.redborder.storm.util.ConfigData;
import net.redborder.storm.util.KafkaConfigFile;
import net.redborder.storm.util.MemcachedConfigFile;
import net.redborder.storm.util.druid.MyBeamFactoryMapFlow;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import trident.memcached.MemcachedState;

public class RedBorderTopology {

    static ConfigData config;
    static KafkaConfigFile kafkaConfig;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {

        String topologyName = "redBorder-Topology";

        if (args.length != 1) {

            System.out.println("./storm jar {name_jar} {main_class} {local|cluster}");

        } else {
            kafkaConfig = new KafkaConfigFile();
            config = new ConfigData(kafkaConfig);

            TridentTopology topology = topology(kafkaConfig.getAvaibleTopics());

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = config.getConfig(args[0]);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, conf, topology.build());

            } else if (args[0].equalsIgnoreCase("cluster")) {

                Config conf = config.getConfig(args[0]);
                StormSubmitter.submitTopology(topologyName, conf, topology.build());
                System.out.println("Topology: " + topologyName + " uploaded successfully.");
            }
        }
    }

    public static TridentTopology topology(List<String> topics) throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        MemcachedConfigFile memConfig = new MemcachedConfigFile();
        MemcachedState.Options memOpts = new MemcachedState.Options();
        memOpts.expiration = 0;
        memOpts.localCacheSize = 0;
        List<String> fields = new ArrayList<>();
        TridentState memcachedState = null;
        int flowPartition = config.getKafkaPartitions("rb_flow");
        int radiusPartition = 0;
        int trapPartition = 0;
        int locationPartition = 0;
        int mobilePartition = 0;
        Stream mseStream = null;
        Stream radiusStream = null;

        StateFactory memcached = MemcachedState.transactional(memConfig.getServers(), memOpts);

        if (topics.contains("rb_loc")) {
            locationPartition = config.getKafkaPartitions("rb_loc");

            // LOCATION DATA
            mseStream = topology.newStream("rb_loc", new TridentKafkaSpout(kafkaConfig, "location").builder())
                    .name("MSE")
                    .each(new Fields("str"), new MapperFunction(), new Fields("mse_map"))
                    .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data", "mse_data_druid"))
                    .parallelismHint(locationPartition);

            memcachedState = mseStream.project(new Fields("src_mac", "mse_data"))
                    .partitionPersist(memcached, new Fields("src_mac", "mse_data"), new MemcachedUpdater("src_mac", "mse_data", "rb_loc"));
        }

        if (topics.contains("rb_mobile")) {
            mobilePartition = config.getKafkaPartitions("rb_mobile");

            // MOBILE DATA
            topology.newStream("rb_mobile", new TridentKafkaSpout(kafkaConfig, "mobile").builder())
                    .name("Mobile")
                    .each(new Fields("str"), new MobileBuilderFunction(), new Fields("key", "mobileMap"))
                    .partitionPersist(memcached, new Fields("key", "mobileMap"), new MemcachedUpdater("key", "mobileMap", "rb_mobile"))
                    .parallelismHint(mobilePartition);
        }

        if (topics.contains("rb_trap")) {
            trapPartition = config.getKafkaPartitions("rb_trap");

            // RSSI DATA
            topology.newStream("rb_trap", new TridentKafkaSpout(kafkaConfig, "trap").builder())
                    .name("RSSI")
                    .each(new Fields("str"), new MapperFunction(), new Fields("rssi"))
                    .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"))
                    .partitionPersist(memcached, new Fields("rssiKey", "rssiValue"), new MemcachedUpdater("rssiKey", "rssiValue", "rb_trap"))
                    .parallelismHint(trapPartition);
        }

        if (topics.contains("rb_radius")) {
            radiusPartition = config.getKafkaPartitions("rb_radius");

            // RADIUS DATA
            radiusStream = topology.newStream("rb_radius", new TridentKafkaSpout(kafkaConfig, "radius").builder())
                    .name("Radius")
                    .each(new Fields("str"), new MapperFunction(), new Fields("radius"))
                    .each(new Fields("radius"), new GetRadiusData(), new Fields("radiusKey", "radiusData", "radiusDruid"))
                    .parallelismHint(radiusPartition);

            radiusStream.project(new Fields("radiusKey", "radiusData"))
                    .partitionPersist(memcached, new Fields("radiusKey", "radiusData"), new MemcachedUpdater("radiusKey", "radiusData", "rb_radius"));
        }
        // FLOW STREAM
        Stream joinedStream = topology.newStream("rb_flow", new TridentKafkaSpout(kafkaConfig, "traffics").builder())
                .parallelismHint(flowPartition).shuffle().name("Main")
                .each(new Fields("str"), new MapperFunction(), new Fields("flows"))
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"));

        fields.add("flows");

        if (topics.contains("rb_loc")) {
            joinedStream = joinedStream.stateQuery(memcachedState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow", "rb_loc"), new Fields("mseMap"));
            fields.add("mseMap");
        }

        if (topics.contains("rb_trap")) {
            joinedStream = joinedStream.stateQuery(memcachedState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow", "rb_trap"), new Fields("rssiMap"));
            fields.add("rssiMap");
        }

        if (topics.contains("rb_radius")) {
            joinedStream = joinedStream.stateQuery(memcachedState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow", "rb_radius"), new Fields("radiusMap"));
            fields.add("radiusMap");
        }

        joinedStream = joinedStream.each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"))
                .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"))
                .each(new Fields("flows"), new AnalizeHttpUrlFunction(), new Fields("httpUrlMap"));

        fields.add("geoIPMap");
        fields.add("macVendorMap");
        fields.add("httpUrlMap");

        if (topics.contains("rb_mobile")) {
            joinedStream = joinedStream.each(new Fields("flows"), new GetFieldFunction("src"), new Fields("src_ip_addr"))
                    .stateQuery(memcachedState, new Fields("src_ip_addr"), new MemcachedQuery("src_ip_addr", "rb_mobile"), new Fields("ipAssignMap"))
                    .each(new Fields("ipAssignMap"), new GetFieldFunction("imsi"), new Fields("imsi"))
                    .stateQuery(memcachedState, new Fields("imsi"), new MemcachedQuery("imsi", "rb_mobile"), new Fields("ueRegisterMap"))
                    .each(new Fields("ueRegisterMap"), new GetFieldFunction("path"), new Fields("path"))
                    .stateQuery(memcachedState, new Fields("path"), new MemcachedQuery("path", "rb_mobile"), new Fields("hnbRegisterMap"));
            fields.add("ipAssignMap");
            fields.add("ueRegisterMap");
            fields.add("hnbRegisterMap");
        }

        joinedStream = joinedStream.each(new Fields(fields), new JoinFlowFunction(), new Fields("finalMap"))
                .project(new Fields("finalMap"))
                .parallelismHint(config.getWorkers());

        String outputTopic = kafkaConfig.getOutputTopic();

        System.out.println("----------------------- Topology info: " + "-----------------------");
        System.out.println("- Storm workers: " + config.getWorkers());
        System.out.println("\n- Kafka partitions: ");
        System.out.println("   * rb_loc: " + locationPartition);
        System.out.println("   * rb_mobile: " + mobilePartition);
        System.out.println("   * rb_trap: " + trapPartition);
        System.out.println("   * rb_flow: " + flowPartition);
        System.out.println("   * rb_radius: " + radiusPartition);

        if (outputTopic != null) {

            int flowPrePartitions = config.getKafkaPartitions(outputTopic);

            System.out.println("   * rb_flow_pre: " + flowPrePartitions);

            System.out.println("Flows send to: " + outputTopic);
            joinedStream
                    .shuffle()
                    .name("Kafka Producer")
                    .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"))
                    .each(new Fields("jsonString"), new ProducerKafkaFunction(kafkaConfig, outputTopic), new Fields("a"))
                    .parallelismHint(flowPrePartitions);

            if (topics.contains("rb_loc")) {
                mseStream
                        .each(new Fields("mse_data_druid"), new MapToJSONFunction(), new Fields("jsonString"))
                        .each(new Fields("jsonString"), new ProducerKafkaFunction(kafkaConfig, outputTopic), new Fields("a"));
            }

            if (topics.contains("rb_radius")) {
                radiusStream
                        .each(new Fields("radiusDruid"), new MapToJSONFunction(), new Fields("radiusJSONString"))
                        .each(new Fields("radiusJSONString"), new ProducerKafkaFunction(kafkaConfig, outputTopic), new Fields("a"));
            }
            
        } else {

            System.out.println("\n- Tranquility info: ");

            int capacity = config.getMiddleManagerCapacity();

            if ((capacity % 2) != 0) {
                capacity = capacity - 1;
            }

            int partitions;
            int replicas;

            if (capacity < 4) {
                partitions = capacity / 2;
                replicas = 1;
            } else {
                partitions = capacity / 4;
                replicas = 2;
            }

            System.out.println("   * partitions: " + partitions);
            System.out.println("   * replicas: " + replicas);

            System.out.println("\nFlows send to indexing service.\n");

            StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(partitions, replicas));

            joinedStream
                    .shuffle()
                    .name("Tranquility")
                    .partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(partitions);

            if (topics.contains("rb_loc")) {
                mseStream.partitionPersist(druidStateFlow, new Fields("mse_data_druid"), new TridentBeamStateUpdater());
            }

            if (topics.contains("rb_radius")) {
                radiusStream.partitionPersist(druidStateFlow, new Fields("radiusDruid"), new TridentBeamStateUpdater());
            }
        }

        System.out.println("\n----------------------- Topology Enrichment-----------------------\n");
        System.out.print("rb_flow --> ");
        if (topics.contains("rb_loc")) {
            System.out.print("rb_loc --> ");
        }

        if (topics.contains("rb_mobile")) {
            System.out.print("rb_mobile --> ");
        }

        if (topics.contains("rb_trap")) {
            System.out.print("rb_trap --> ");
        }

        if (topics.contains("rb_radius")) {
            System.out.print("rb_radius --> ");
        }

        System.out.println("||\n");

        return topology;
    }

}
