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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
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

    static ConfigData _config;
    static KafkaConfigFile _kafkaConfig;
    static RiakConfigFile _riakConfig;
    static String _outputTopic;
    static int _tranquilityPartitions = 0;
    static int _tranquilityReplicas = 0;
    static boolean _debug = false;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        String topologyName = "redBorder-Topology";
        TridentTopology topology = null;

        if (args.length < 1) {

            System.out.println("./storm jar {name_jar} {main_class} {local|cluster} [_debug]");
        } else {
            if (args.length == 2) {
                if (args[1].equals("_debug")) {
                    _debug = true;
                    System.out.println("_debug mode: ON");
                } else {
                    System.out.println("./storm jar {name_jar} {main_class} {local|cluster} [_debug]");
                }
            }

            init();
            try {
                 topology = topology();
            }catch (IOException ex){
                System.out.println("Error writting info file:" +ex);
                System.exit(1);
            }

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = _config.getConfig(args[0]);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, conf, topology.build());

            } else if (args[0].equalsIgnoreCase("cluster")) {
                Config conf = _config.getConfig(args[0]);
                StormSubmitter.submitTopology(topologyName, conf, topology.build());
                System.out.println("\nTopology: " + topologyName + " uploaded successfully.");
            }
        }
    }

    private static void init() {
        // Config vars
        _kafkaConfig = new KafkaConfigFile(_debug);
        _riakConfig = new RiakConfigFile(_debug);
        _config = new ConfigData(_kafkaConfig);

        // Get kafka output topic
        _kafkaConfig.setSection("traffics");
        _outputTopic = _kafkaConfig.getOutputTopic();

        // Get tranquility configuration
        if (_outputTopic == null) {
            int capacity = _config.getMiddleManagerCapacity();

            if ((capacity % 2) != 0) capacity = capacity - 1;

            if (capacity < 4) {
                _tranquilityPartitions = capacity / 2;
                _tranquilityReplicas = 1;
            } else {
                _tranquilityPartitions = capacity / 4;
                _tranquilityReplicas = 2;
            }
        }
    }

    public static TridentTopology topology() throws IOException {
        TridentTopology topology = new TridentTopology();
        List<String> fields = new ArrayList<>();
        List<String> topics = _kafkaConfig.getAvaibleTopics();

        /* States and Streams*/
        TridentState locationState, mobileState, radiusState, trapState, darklistState;
        RiakState.Factory locationStateFactory, mobileStateFactory, trapStateFactory, radiusStateFactory;
        Stream locationStream, radiusStream;

        /* Partitions */
        int flowPartition = _config.getKafkaPartitions("rb_flow");
        int radiusPartition, trapPartition, locationPartition, mobilePartition;
        radiusPartition = trapPartition = locationPartition = mobilePartition = 0;

        /*
         *  Flow
         */

        Stream mainStream = topology.newStream("rb_flow", new TridentKafkaSpout(_kafkaConfig, "traffics").builder())
                .parallelismHint(flowPartition).shuffle().name("Main")
                .each(new Fields("str"), new MapperFunction(_debug), new Fields("flows"))
                .each(new Fields("flows"), new MacVendorFunction(_debug), new Fields("macVendorMap"))
                .each(new Fields("flows"), new GeoIpFunction(_debug), new Fields("geoIPMap"))
                .each(new Fields("flows"), new AnalizeHttpUrlFunction(_debug), new Fields("httpUrlMap"));

        fields.add("flows");
        fields.add("geoIPMap");
        fields.add("macVendorMap");
        fields.add("httpUrlMap");

        /*
         *  Location
         */

        if (topics.contains("rb_loc")) {
            locationPartition = _config.getKafkaPartitions("rb_loc");
            locationStateFactory = new RiakState.Factory<>("rbbi:location", _riakConfig.getServers(), 8087, Map.class);
            locationState = topology.newStaticState(locationStateFactory);

            // Get msg
            locationStream = topology.newStream("rb_loc", new TridentKafkaSpout(_kafkaConfig, "location").builder())
                    .name("Location").parallelismHint(locationPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction(_debug), new Fields("mse_map"))
                    .each(new Fields("mse_map"), new GetMSEdata(_debug), new Fields("src_mac", "mse_data", "mse_data_druid"))
                    .each(new Fields("mse_data_druid"), new MacVendorFunction(_debug), new Fields("mseMacVendorMap"))
                    .each(new Fields("mse_data_druid"), new GeoIpFunction(_debug), new Fields("mseGeoIPMap"));

            // Save it to enrich later on
            locationStream.partitionPersist(locationStateFactory, new Fields("src_mac", "mse_data"),
                    new RiakUpdater("src_mac", "mse_data", _debug));

            // Generate a flow msg
            persist(locationStream.each(new Fields("mse_data_druid", "mseMacVendorMap", "mseGeoIPMap"),
                    new JoinFlowFunction(_debug), new Fields("finalMap")));

            // Enrich flow stream
            mainStream = mainStream.stateQuery(locationState, new Fields("flows"),
                    new RiakQuery("client_mac", _debug), new Fields("mseMap"));

            fields.add("mseMap");
        }

        /*
         *  Mobile
         */

        if (topics.contains("rb_mobile")) {
            mobilePartition = _config.getKafkaPartitions("rb_mobile");
            mobileStateFactory = new RiakState.Factory<>("rbbi:mobile", _riakConfig.getServers(), 8087, Map.class);
            mobileState = topology.newStaticState(mobileStateFactory);

            // Get msg and save it to enrich later on
            topology.newStream("rb_mobile", new TridentKafkaSpout(_kafkaConfig, "mobile").builder())
                    .name("Mobile").parallelismHint(mobilePartition).shuffle()
                    .each(new Fields("str"), new MobileBuilderFunction(_debug), new Fields("key", "mobileMap"))
                    .partitionPersist(mobileStateFactory, new Fields("key", "mobileMap"), new RiakUpdater("key", "mobileMap", _debug));

            // Enrich flow stream
            mainStream = mainStream
                    .stateQuery(mobileState, new Fields("flows"), new RiakQuery("src", _debug), new Fields("ipAssignMap"))
                    .stateQuery(mobileState, new Fields("ipAssignMap"), new RiakQuery("imsi", _debug), new Fields("ueRegisterMap"))
                    .stateQuery(mobileState, new Fields("ueRegisterMap"), new RiakQuery("path", _debug), new Fields("hnbRegisterMap"));

            fields.add("ipAssignMap");
            fields.add("ueRegisterMap");
            fields.add("hnbRegisterMap");
        }

        /*
         *  Trap
         */

        if (topics.contains("rb_trap")) {
            trapPartition = _config.getKafkaPartitions("rb_trap");
            trapStateFactory = new RiakState.Factory<>("rbbi:trap", _riakConfig.getServers(), 8087, Map.class);
            trapState = topology.newStaticState(trapStateFactory);

            // Get msg and save it to enrich later on
            topology.newStream("rb_trap", new TridentKafkaSpout(_kafkaConfig, "trap").builder())
                    .name("Trap").parallelismHint(trapPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction(_debug), new Fields("rssi"))
                    .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"))
                    .partitionPersist(trapStateFactory, new Fields("rssiKey", "rssiValue"), new RiakUpdater("rssiKey", "rssiValue", _debug));

            // Enrich flow stream
            mainStream = mainStream
                    .stateQuery(trapState, new Fields("flows"), new RiakQuery("client_mac", _debug), new Fields("rssiMap"));

            fields.add("rssiMap");
        }

        /*
         *  Radius
         */

        if (topics.contains("rb_radius")) {
            radiusPartition = _config.getKafkaPartitions("rb_radius");
            radiusStateFactory = new RiakState.Factory<>("rbbi:radius", _riakConfig.getServers(), 8087, Map.class);
            radiusState = topology.newStaticState(radiusStateFactory);

            // Get msg
            radiusStream = topology.newStream("rb_radius", new TridentKafkaSpout(_kafkaConfig, "radius").builder())
                    .name("Radius").parallelismHint(radiusPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction(_debug), new Fields("radius"));

            if (_kafkaConfig.getOverwriteCache("radius")) {
                // Get the radius data from the radius message
                radiusStream = radiusStream
                        .each(new Fields("radius"), new GetRadiusData(_debug),
                                new Fields("radiusKey", "radiusData", "radiusDruid"));

            } else {
                // Get the current radius data from that client and merge it with the data
                // specified on the radius message
                radiusStream = radiusStream
                        .each(new Fields("radius"), new GetRadiusClient(_debug), new Fields("clientMap"))
                        .stateQuery(radiusState, new Fields("clientMap"), new RiakQuery("client_mac", _debug),
                                new Fields("radiusCached"))
                        .each(new Fields("radius", "radiusCached"), new GetRadiusData(_debug),
                                new Fields("radiusKey", "radiusData", "radiusDruid"));
            }

            // Save msg to enrich later on
            radiusState = radiusStream.project(new Fields("radiusKey", "radiusData"))
                    .partitionPersist(radiusStateFactory, new Fields("radiusKey", "radiusData"),
                            new RiakUpdater("radiusKey", "radiusData", _debug));

            // Generate a flow msg
            persist(radiusStream
                    .each(new Fields("radiusDruid"), new MacVendorFunction(_debug), new Fields("radiusMacVendorMap"))
                    .each(new Fields("radiusDruid"), new GeoIpFunction(_debug), new Fields("radiusGeoIPMap"))
                    .each(new Fields("radiusDruid", "radiusMacVendorMap", "radiusGeoIPMap"),
                            new JoinFlowFunction(_debug), new Fields("finalMap")));

            // Enrich flow stream
            mainStream = mainStream.stateQuery(radiusState, new Fields("flows"), new RiakQuery("client_mac", _debug),
                    new Fields("radiusMap"));

            fields.add("radiusMap");
        }

        /*
         *  Darklist
         */

        if (_kafkaConfig.getDarkList()) {
            // Create a static state to query the database
            darklistState = topology.newStaticState(new RiakState.Factory<>("rbbi:darklist",
                    _riakConfig.getServers(), 8087, Map.class));

            // Enrich flow stream with darklist fields
            mainStream = mainStream
                    .stateQuery(darklistState, new Fields("flows"), new RiakQuery("src", _debug),
                            new Fields("darklistMap"));

            fields.add("darklistMap");
        }

        /*
         *  Join fields and persist
         */

        persist(mainStream.each(new Fields(fields), new JoinFlowFunction(_debug), new Fields("finalMap"))
                .project(new Fields("finalMap"))
                .parallelismHint(_config.getWorkers())
                .shuffle().name("Producer")
                .each(new Fields(), new ThroughputLoggingFilter()));

        /*
         *  Show info
         */

        FileWriter file = new FileWriter("/opt/rb/var/redBorder-BI/app/topologyInfo");
        PrintWriter pw = new PrintWriter(file);

        pw.println("----------------------- Topology info: " + "-----------------------");
        pw.println("- Date topology: " + new Date().toString());
        pw.println("- Storm workers: " + _config.getWorkers());
        pw.println("\n- Kafka partitions: ");

        System.out.println("----------------------- Topology info: " + "-----------------------");
        System.out.println("- Storm workers: " + _config.getWorkers());
        System.out.println("\n- Kafka partitions: ");

        if (locationPartition > 0) {
            System.out.println("   * rb_loc: " + locationPartition);
            pw.println("   * rb_loc: " + locationPartition);
        }
        if (mobilePartition > 0) {
            System.out.println("   * rb_mobile: " + mobilePartition);
            pw.println("   * rb_mobile: " + mobilePartition);
        }
        if (trapPartition > 0) {
            System.out.println("   * rb_trap: " + trapPartition);
            pw.println("   * rb_trap: " + trapPartition);
        }
        if (flowPartition > 0) {
            System.out.println("   * rb_flow: " + flowPartition);
            pw.println("   * rb_flow: " + flowPartition);
        }
        if (radiusPartition > 0) {
            System.out.println("   * rb_radius: " + radiusPartition);
            pw.println("   * rb_radius: " + radiusPartition);
        }

        System.out.println("\n- Zookeeper Servers: " + _kafkaConfig.getZkHost());
        pw.println("\n- Zookeeper Servers: " + _kafkaConfig.getZkHost());

        System.out.println("\n- Riak Servers: " + _riakConfig.getServers().toString());
        pw.println("\n- Riak Servers: " + _riakConfig.getServers().toString());

        if (_outputTopic != null) {
            System.out.println("   * " + _outputTopic + ": " + _config.getKafkaPartitions(_outputTopic));
            System.out.println("Flows send to (kafka topic): " + _outputTopic);

            pw.println("   * " + _outputTopic + ": " + _config.getKafkaPartitions(_outputTopic));
            pw.println("Flows send to (kafka topic): " + _outputTopic);
        } else {
            System.out.println("\n- Tranquility info: ");
            System.out.println("   * partitions: " + _tranquilityPartitions);
            System.out.println("   * replicas: " + _tranquilityReplicas);
            System.out.println("\n Flows send to indexing service. \n");

            pw.println("\n- Tranquility info: ");
            pw.println("   * partitions: " + _tranquilityPartitions);
            pw.println("   * replicas: " + _tranquilityReplicas);
            pw.println("\n Flows send to indexing service. \n");
        }

        System.out.println("\n----------------------- Topology Enrichment-----------------------\n");
        System.out.println(" - flow: ");
        System.out.println("   * location: " + getEnrichment(topics.contains("rb_loc")));
        System.out.println("   * mobile: " + getEnrichment(topics.contains("rb_mobile")));
        System.out.println("   * trap: " + getEnrichment(topics.contains("rb_trap")));
        System.out.println("   * radius (overwrite_cache: " + _kafkaConfig.getOverwriteCache("radius") + ") : " + getEnrichment(topics.contains("rb_radius")));
        System.out.println("   * darklist: " + getEnrichment(_kafkaConfig.getDarkList()));
        System.out.println();

        pw.println("\n----------------------- Topology Enrichment-----------------------\n");
        pw.println(" - flow: ");
        pw.println("   * location: " + getEnrichment(topics.contains("rb_loc")));
        pw.println("   * mobile: " + getEnrichment(topics.contains("rb_mobile")));
        pw.println("   * trap: " + getEnrichment(topics.contains("rb_trap")));
        pw.println("   * radius (overwrite_cache: " + _kafkaConfig.getOverwriteCache("radius") + ") : " + getEnrichment(topics.contains("rb_radius")));
        pw.println("   * darklist: " + getEnrichment(_kafkaConfig.getDarkList()));
        pw.println();

        pw.flush();

        return topology;
    }

    private static TridentState persist(Stream s) {
        TridentState ret;

        if (_outputTopic != null) {
            int flowPrePartitions = _config.getKafkaPartitions(_outputTopic);

            ret = s.each(new Fields("finalMap"), new MapToJSONFunction(_debug), new Fields("jsonString"))
                    .partitionPersist(KafkaState.nonTransactional(_kafkaConfig.getZkHost()),
                            new Fields("jsonString"), new KafkaStateUpdater("jsonString", _outputTopic))
                    .parallelismHint(flowPrePartitions);
        } else {
            StateFactory druidStateFlow = new TridentBeamStateFactory<>(
                    new MyBeamFactoryMapFlow(_tranquilityPartitions, _tranquilityReplicas, _debug));

            ret = s.partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(_tranquilityPartitions);
        }

        return ret;
    }

    private static String getEnrichment(boolean bool) {
        if (bool) {
            return "✓";
        } else {
            return "x";
        }
    }

}
