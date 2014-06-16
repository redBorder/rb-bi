package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import com.github.quintona.KafkaState;
import com.github.quintona.KafkaStateUpdater;
import com.metamx.tranquility.storm.TridentBeamStateFactory;
import com.metamx.tranquility.storm.TridentBeamStateUpdater;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.redborder.storm.function.*;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.state.*;
import net.redborder.storm.util.ConfigData;
import net.redborder.storm.util.KafkaConfigFile;
import net.redborder.storm.util.RiakConfigFile;
import net.redborder.storm.util.druid.MyBeamFactoryMapFlow;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

public class RedBorderTopology {

    static ConfigData config;
    static KafkaConfigFile kafkaConfig;
    static boolean debug = false;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {

        String topologyName = "redBorder-Topology";

        if (args.length < 1) {

            System.out.println("./storm jar {name_jar} {main_class} {local|cluster} [debug]");
        } else {
            if (args.length == 2) {
                if (args[1].equals("debug")) {
                    debug = true;
                    System.out.println("Debug mode: ON");
                } else {
                    System.out.println("./storm jar {name_jar} {main_class} {local|cluster} [debug]");
                }
            }

            kafkaConfig = new KafkaConfigFile(debug);
            config = new ConfigData(kafkaConfig);

            TridentTopology topology = topology(kafkaConfig.getAvaibleTopics(), debug);

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = config.getConfig(args[0]);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, conf, topology.build());

            } else if (args[0].equalsIgnoreCase("cluster")) {
                Config conf = config.getConfig(args[0]);
                StormSubmitter.submitTopology(topologyName, conf, topology.build());
                System.out.println("\nTopology: " + topologyName + " uploaded successfully.");
            }
        }
    }

    public static TridentTopology topology(List<String> topics, boolean debug) throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        List<String> fields = new ArrayList<>();
        RiakConfigFile riakConfig = new RiakConfigFile(debug);

        /*
            States
         */
        TridentState locationState = null;
        TridentState mobileState = null;
        TridentState radiusState = topology.newStaticState(new RiakState.Factory("rbbi:radius", riakConfig.getServers(), 8087, Map.class));
        TridentState trapState = null;
        TridentState darklistState = topology.newStaticState(new RiakState.Factory("rbbi:darklist", riakConfig.getServers(), 8087, Map.class));

        /*
            Streams
         */
        Stream locationStream = null;
        Stream radiusStream = null;
        Stream mobileStream = null;
        Stream trapStream = null;

        /*
            Partitions
         */
        int flowPartition = config.getKafkaPartitions("rb_flow");
        int radiusPartition = 0;
        int trapPartition = 0;
        int locationPartition = 0;
        int mobilePartition = 0;


        if (topics.contains("rb_loc")) {
            locationPartition = config.getKafkaPartitions("rb_loc");

            // LOCATION DATA
            locationStream = topology.newStream("rb_loc", new TridentKafkaSpout(kafkaConfig, "location").builder())
                    .name("MSE")
                    .each(new Fields("str"), new MapperFunction(debug), new Fields("mse_map"))
                    .each(new Fields("mse_map"), new GetMSEdata(debug), new Fields("src_mac", "mse_data", "mse_data_druid"));

            locationState = locationStream.project(new Fields("src_mac", "mse_data"))
                    .partitionPersist(new RiakState.Factory("rbbi:location", riakConfig.getServers(), 8087, Map.class), new Fields("src_mac", "mse_data"), new RiakUpdater("src_mac", "mse_data", debug));
        }

        if (topics.contains("rb_mobile")) {
            mobilePartition = config.getKafkaPartitions("rb_mobile");

            // MOBILE DATA
            mobileStream = topology.newStream("rb_mobile", new TridentKafkaSpout(kafkaConfig, "mobile").builder())
                    .name("Mobile")
                    .parallelismHint(locationPartition)
                    .shuffle()
                    .each(new Fields("str"), new MobileBuilderFunction(debug), new Fields("key", "mobileMap"))
                    .parallelismHint(mobilePartition);

            mobileState = mobileStream
                    .partitionPersist(new RiakState.Factory("rbbi:mobile", riakConfig.getServers(), 8087, Map.class), new Fields("key", "mobileMap"), new RiakUpdater("key", "mobileMap", debug));

        }

        if (topics.contains("rb_trap")) {
            trapPartition = config.getKafkaPartitions("rb_trap");

            // RSSI DATA
            trapStream = topology.newStream("rb_trap", new TridentKafkaSpout(kafkaConfig, "trap").builder())
                    .name("RSSI")
                    .parallelismHint(trapPartition)
                    .shuffle()
                    .each(new Fields("str"), new MapperFunction(debug), new Fields("rssi"))
                    .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"));

            trapState = trapStream
                    .partitionPersist(new RiakState.Factory("rbbi:trap", riakConfig.getServers(), 8087, Map.class), new Fields("rssiKey", "rssiValue"), new RiakUpdater("rssiKey", "rssiValue", debug));

        }

        if (topics.contains("rb_radius")) {
            radiusPartition = config.getKafkaPartitions("rb_radius");

            radiusStream = topology.newStream("rb_radius", new TridentKafkaSpout(kafkaConfig, "radius").builder())
                    .name("Radius")
                    .parallelismHint(radiusPartition)
                    .shuffle()
                    .each(new Fields("str"), new MapperFunction(debug), new Fields("radius"));

            // RADIUS DATA
            if (kafkaConfig.getOverwriteCache("radius")) {
                radiusStream = radiusStream
                        .each(new Fields("radius"), new GetRadiusData(debug), new Fields("radiusKey", "radiusData", "radiusDruid"));

            } else {

                radiusStream = radiusStream
                        .each(new Fields("radius"), new GetRadiusClient(debug), new Fields("clientMap"))
                        .stateQuery(radiusState, new Fields("clientMap"), new RiakQuery("client_mac", debug), new Fields("radiusCached"))
                        .each(new Fields("radius", "radiusCached"), new GetRadiusData(debug), new Fields("radiusKey", "radiusData", "radiusDruid"));

            }

            radiusState = radiusStream.project(new Fields("radiusKey", "radiusData"))
                    .partitionPersist(new RiakState.Factory("rbbi:radius", riakConfig.getServers(), 8087, Map.class), new Fields("radiusKey", "radiusData"), new RiakUpdater("radiusKey", "radiusData", debug));
        }
        // FLOW STREAM
        Stream mainStream = topology.newStream("rb_flow", new TridentKafkaSpout(kafkaConfig, "traffics").builder())
                .parallelismHint(flowPartition).shuffle().name("Main")
                .each(new Fields("str"), new MapperFunction(debug), new Fields("flows"));

        fields.add("flows");

        if (topics.contains("rb_loc")) {
            mainStream = mainStream
                    .stateQuery(locationState, new Fields("flows"), new RiakQuery("client_mac", debug), new Fields("mseMap"));
            fields.add("mseMap");
        }

        if (topics.contains("rb_trap")) {
            mainStream = mainStream.stateQuery(trapState, new Fields("flows"), new RiakQuery("client_mac", debug), new Fields("rssiMap"));
            fields.add("rssiMap");
        }

        if (topics.contains("rb_radius")) {
            mainStream = mainStream.stateQuery(radiusState, new Fields("flows"), new RiakQuery("client_mac", debug), new Fields("radiusMap"));
            fields.add("radiusMap");
        }

        mainStream = mainStream.each(new Fields("flows"), new MacVendorFunction(debug), new Fields("macVendorMap"))
                .each(new Fields("flows"), new GeoIpFunction(debug), new Fields("geoIPMap"))
                .each(new Fields("flows"), new AnalizeHttpUrlFunction(debug), new Fields("httpUrlMap"));

        fields.add("geoIPMap");
        fields.add("macVendorMap");
        fields.add("httpUrlMap");

        if (topics.contains("rb_mobile")) {
            mainStream = mainStream
                    .stateQuery(mobileState, new Fields("flows"), new RiakQuery("src", debug), new Fields("ipAssignMap"))
                    .stateQuery(mobileState, new Fields("ipAssignMap"), new RiakQuery("imsi", debug), new Fields("ueRegisterMap"))
                    .stateQuery(mobileState, new Fields("ueRegisterMap"), new RiakQuery("path", debug), new Fields("hnbRegisterMap"));

            fields.add("ipAssignMap");
            fields.add("ueRegisterMap");
            fields.add("hnbRegisterMap");
        }

        if (kafkaConfig.getDarkList()) {
            mainStream = mainStream
                    .stateQuery(darklistState, new Fields("flows"), new RiakQuery("src", debug), new Fields("darklistMap"));

            fields.add("darklistMap");
        }

        mainStream = mainStream.each(new Fields(fields), new JoinFlowFunction(debug), new Fields("finalMap"))
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
            System.out.println("   * " + outputTopic + ": " + flowPrePartitions);
            System.out.println("Flows send to: " + outputTopic);

            mainStream
                    .shuffle().name("Kafka Producer")
                    .each(new Fields("finalMap"), new MapToJSONFunction(debug), new Fields("jsonString"))
                    .each(new Fields(), new ThroughputLoggingFilter())
                    .partitionPersist(KafkaState.nonTransactional(kafkaConfig.getZkHost()), new Fields("jsonString"), new KafkaStateUpdater("jsonString", outputTopic))
                    .parallelismHint(flowPrePartitions);

            if (topics.contains("rb_loc")) {
                locationStream
                        .each(new Fields("mse_data_druid"), new MacVendorFunction(debug), new Fields("mseMacVendorMap"))
                        .each(new Fields("mse_data_druid"), new GeoIpFunction(debug), new Fields("mseGeoIPMap"))
                        .each(new Fields("mse_data_druid", "mseMacVendorMap", "mseGeoIPMap"), new JoinFlowFunction(debug), new Fields("mseFinalMap"))
                        .each(new Fields("mseFinalMap"), new MapToJSONFunction(debug), new Fields("mseJsonString"))
                        .partitionPersist(KafkaState.nonTransactional(kafkaConfig.getZkHost()), new Fields("mseJsonString"), new KafkaStateUpdater("jsonString", outputTopic));
            }

            if (topics.contains("rb_radius")) {
                radiusStream
                        .each(new Fields("radiusDruid"), new MacVendorFunction(debug), new Fields("radiusMacVendorMap"))
                        .each(new Fields("radiusDruid"), new GeoIpFunction(debug), new Fields("radiusGeoIPMap"))
                        .each(new Fields("radiusDruid", "radiusMacVendorMap", "radiusGeoIPMap"), new JoinFlowFunction(debug), new Fields("radiusFinalMap"))
                        .each(new Fields("radiusFinalMap"), new MapToJSONFunction(debug), new Fields("radiusJSONString"))
                        .partitionPersist(KafkaState.nonTransactional(kafkaConfig.getZkHost()), new Fields("radiusJSONString"), new KafkaStateUpdater("radiusJSONString", outputTopic));
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

            StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(partitions, replicas, debug));

            mainStream
                    .shuffle().name("Tranquility")
                    .each(new Fields(), new ThroughputLoggingFilter())
                    .partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(partitions);

            if (topics.contains("rb_loc")) {
                locationStream
                        .each(new Fields("mse_data_druid"), new MacVendorFunction(debug), new Fields("mseMacVendorMap"))
                        .each(new Fields("mse_data_druid"), new GeoIpFunction(debug), new Fields("mseGeoIPMap"))
                        .each(new Fields("mse_data_druid", "mseMacVendorMap", "mseGeoIPMap"), new JoinFlowFunction(debug), new Fields("mseFinalMap"))
                        .partitionPersist(druidStateFlow, new Fields("mseFinalMap"), new TridentBeamStateUpdater());
            }

            if (topics.contains("rb_radius")) {
                radiusStream
                        .each(new Fields("radiusDruid"), new MacVendorFunction(debug), new Fields("radiusMacVendorMap"))
                        .each(new Fields("radiusDruid"), new GeoIpFunction(debug), new Fields("radiusGeoIPMap"))
                        .each(new Fields("radiusDruid", "radiusMacVendorMap", "radiusGeoIPMap"), new JoinFlowFunction(debug), new Fields("radiusFinalMap"))
                        .partitionPersist(druidStateFlow, new Fields("radiusFinalMap"), new TridentBeamStateUpdater());
            }
        }

        System.out.println("\n----------------------- Topology Enrichment-----------------------\n");
        System.out.println(" - flow: ");
        if (topics.contains("rb_loc")) {
            System.out.println("   * location: ✓ ");
        } else {
            System.out.println("   * location: x");
        }

        if (topics.contains("rb_mobile")) {
            System.out.println("   * mobile: ✓ ");
        } else {
            System.out.println("   * mobile: x");
        }

        if (topics.contains("rb_trap")) {
            System.out.println("   * trap: ✓");
        } else {
            System.out.println("   * trap: x");
        }

        if (topics.contains("rb_radius")) {
            System.out.println("   * radius " + "(overwrite_cache: " + kafkaConfig.getOverwriteCache("radius") + ") : ✓ ");
        } else {
            System.out.println("   * radius: x");
        }

        if (kafkaConfig.getDarkList()) {
            System.out.println("   * darklist: ✓");
        } else {
            System.out.println("   * darklist: x");
        }

        return topology;
    }

}
