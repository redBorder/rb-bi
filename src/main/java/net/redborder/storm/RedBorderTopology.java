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
import net.redborder.storm.util.druid.MyBeamFactoryMapEvent;
import net.redborder.storm.util.druid.MyBeamFactoryMapFlow;
import net.redborder.storm.util.druid.MyBeamFactoryMapMonitor;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

public class RedBorderTopology {

    static ConfigData _config;
    static KafkaConfigFile _kafkaConfig;
    static RiakConfigFile _riakConfig;
    static String _outputTopic, _outputTopicEvent, _outputTopicMonitor;
    static int _tranquilityPartitions = 0;
    static int _tranquilityReplicas = 0;
    static boolean _debug = false;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String topologyName = "redBorder-Topology";

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
            TridentTopology topology = topology();

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
        int tranquilitySections = 0;
        _outputTopic = _kafkaConfig.getOutputTopic("traffics");
        if (_outputTopic == null) tranquilitySections++;
        _outputTopicEvent = _kafkaConfig.getOutputTopic("events");
        if (_outputTopicEvent == null) tranquilitySections++;
        _outputTopicMonitor = _kafkaConfig.getOutputTopic("monitor");
        // if (_outputTopicMonitor == null) tranquilitySections++;

        // Get tranquility configuration
        if (tranquilitySections > 0) {
           int capacity = _config.getMiddleManagerCapacity();

            if(_kafkaConfig.contains("monitor")){
                capacity = capacity - 2;
            }

            capacity = capacity /tranquilitySections;

            if ((capacity % 2) != 0) capacity = capacity - 1;

            if (capacity <= 2 * tranquilitySections) {
                _tranquilityPartitions = 1;
                _tranquilityReplicas = 1;
            } else {
                _tranquilityPartitions = capacity / 4;
                _tranquilityReplicas = 2;
            }

            _tranquilityPartitions = 1;
            _tranquilityReplicas = 1;
        }
    }

    public static TridentTopology topology() {
        TridentTopology topology = new TridentTopology();
        List<String> fieldsFlow = new ArrayList<>();
        List<String> fieldsEvent = new ArrayList<>();

        /* States and Streams*/
        TridentState locationState, mobileState, radiusState, trapState, darklistState;
        RiakState.Factory locationStateFactory, mobileStateFactory, trapStateFactory, radiusStateFactory;
        Stream locationStream = null, radiusStream = null, eventsStream = null, monitorStream = null;

        /* Partitions */
        int flowPartition = _config.getKafkaPartitions("rb_flow");
        int eventsPartition = _config.getKafkaPartitions("rb_event");
        int monitorPartition = _config.getKafkaPartitions("rb_monitor");
        int radiusPartition, trapPartition, locationPartition, mobilePartition;
        radiusPartition = trapPartition = locationPartition = mobilePartition = 0;

        /*
         *  Flow
         */


        Stream flowStream = topology.newStream("rb_flow", new TridentKafkaSpout(_kafkaConfig, "traffics").builder())
                .parallelismHint(flowPartition).shuffle().name("Flows")
                .each(new Fields("str"), new MapperFunction(_debug, "rb_flow"), new Fields("flows"))
                .each(new Fields("flows"), new MacVendorFunction(_debug), new Fields("macVendorMap"))
                .each(new Fields("flows"), new GeoIpFunction(_debug), new Fields("geoIPMap"))
                .each(new Fields("flows"), new AnalizeHttpUrlFunction(_debug), new Fields("httpUrlMap"));

        fieldsFlow.add("flows");
        fieldsFlow.add("geoIPMap");
        fieldsFlow.add("macVendorMap");
        fieldsFlow.add("httpUrlMap");

        /*
         *  Events
         */

        if (_kafkaConfig.contains("events")) {

            eventsStream = topology.newStream("rb_event", new TridentKafkaSpout(_kafkaConfig, "events").builder())
                    .parallelismHint(eventsPartition).shuffle().name("Events")
                    .each(new Fields("str"), new MapperFunction(_debug, "rb_event"), new Fields("event"))
                    .each(new Fields("event"), new MacVendorFunction(_debug), new Fields("macVendorMap"))
                    .each(new Fields("event"), new GeoIpFunction(_debug), new Fields("geoIPMap"));

            fieldsEvent.add("event");
            fieldsEvent.add("macVendorMap");
            fieldsEvent.add("geoIPMap");
        }

        /*
         *  Monitor
         */

        if (_kafkaConfig.contains("monitor")) {
            locationPartition = _config.getKafkaPartitions("rb_loc");

            monitorStream = topology.newStream("rb_monitor", new TridentKafkaSpout(_kafkaConfig, "monitor").builder())
                    .parallelismHint(monitorPartition).shuffle().name("Monitor")
                    .each(new Fields("str"), new MapperFunction(_debug, "rb_monitor"), new Fields("finalMap"));

        }

        /*
         *  Location
         */

        if (_kafkaConfig.contains("location")) {
            locationPartition = _config.getKafkaPartitions("rb_loc");
            locationStateFactory = new RiakState.Factory<>("rbbi:location", _riakConfig.getServers(), 8087, Map.class);
            locationState = topology.newStaticState(locationStateFactory);

            // Get msg
            locationStream = topology.newStream("rb_loc", new TridentKafkaSpout(_kafkaConfig, "location").builder())
                    .name("Location").parallelismHint(locationPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction(_debug, "rb_loc"), new Fields("mse_map"))
                    .each(new Fields("mse_map"), new GetMSEdata(_debug), new Fields("src_mac", "mse_data", "mse_data_druid"))
                    .each(new Fields("mse_data_druid"), new MacVendorFunction(_debug), new Fields("mseMacVendorMap"))
                    .each(new Fields("mse_data_druid"), new GeoIpFunction(_debug), new Fields("mseGeoIPMap"));

            // Save it to enrich later on
            locationStream.partitionPersist(locationStateFactory, new Fields("src_mac", "mse_data"),
                    new RiakUpdater("src_mac", "mse_data", _debug));

            // Generate a flow msg
            persist(locationStream.each(new Fields("mse_data_druid", "mseMacVendorMap", "mseGeoIPMap"),
                    new MergeMapsFunction(_debug), new Fields("finalMap")));

            // Enrich flow stream
            flowStream = flowStream.stateQuery(locationState, new Fields("flows"),
                    new RiakQuery("client_mac", _debug), new Fields("mseMap"));

            fieldsFlow.add("mseMap");
        }

        /*
         *  Mobile
         */

        if (_kafkaConfig.contains("mobile")) {
            mobilePartition = _config.getKafkaPartitions("rb_mobile");
            mobileStateFactory = new RiakState.Factory<>("rbbi:mobile", _riakConfig.getServers(), 8087, Map.class);
            mobileState = topology.newStaticState(mobileStateFactory);

            // Get msg and save it to enrich later on
            topology.newStream("rb_mobile", new TridentKafkaSpout(_kafkaConfig, "mobile").builder())
                    .name("Mobile").parallelismHint(mobilePartition).shuffle()
                    .each(new Fields("str"), new MobileBuilderFunction(_debug), new Fields("key", "mobileMap"))
                    .partitionPersist(mobileStateFactory, new Fields("key", "mobileMap"), new RiakUpdater("key", "mobileMap", _debug));

            // Enrich flow stream
            flowStream = flowStream
                    .stateQuery(mobileState, new Fields("flows"), new RiakQuery("src", _debug), new Fields("ipAssignMap"))
                    .stateQuery(mobileState, new Fields("ipAssignMap"), new RiakQuery("imsi", _debug), new Fields("ueRegisterMap"))
                    .stateQuery(mobileState, new Fields("ueRegisterMap"), new RiakQuery("path", _debug), new Fields("hnbRegisterMap"));

            fieldsFlow.add("ipAssignMap");
            fieldsFlow.add("ueRegisterMap");
            fieldsFlow.add("hnbRegisterMap");
        }

        /*
         *  Trap
         */

        if (_kafkaConfig.contains("trap")) {
            trapPartition = _config.getKafkaPartitions("rb_trap");
            trapStateFactory = new RiakState.Factory<>("rbbi:trap", _riakConfig.getServers(), 8087, Map.class);
            trapState = topology.newStaticState(trapStateFactory);

            // Get msg and save it to enrich later on
            topology.newStream("rb_trap", new TridentKafkaSpout(_kafkaConfig, "trap").builder())
                    .name("Trap").parallelismHint(trapPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction(_debug, "rb_trap"), new Fields("rssi"))
                    .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"))
                    .partitionPersist(trapStateFactory, new Fields("rssiKey", "rssiValue"), new RiakUpdater("rssiKey", "rssiValue", _debug));

            // Enrich flow stream
            flowStream = flowStream
                    .stateQuery(trapState, new Fields("flows"), new RiakQuery("client_mac", _debug), new Fields("rssiMap"));

            fieldsFlow.add("rssiMap");
        }

        /*
         *  Radius
         */

        if (_kafkaConfig.contains("radius")) {
            radiusPartition = _config.getKafkaPartitions("rb_radius");
            radiusStateFactory = new RiakState.Factory<>("rbbi:radius", _riakConfig.getServers(), 8087, Map.class);
            radiusState = topology.newStaticState(radiusStateFactory);

            // Get msg
            radiusStream = topology.newStream("rb_radius", new TridentKafkaSpout(_kafkaConfig, "radius").builder())
                    .name("Radius").parallelismHint(radiusPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction(_debug, "rb_radius"), new Fields("radius"));

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
                            new MergeMapsFunction(_debug), new Fields("finalMap")));

            // Enrich flow stream
            flowStream = flowStream.stateQuery(radiusState, new Fields("flows"), new RiakQuery("client_mac", _debug),
                    new Fields("radiusMap"));

            fieldsFlow.add("radiusMap");
        }

        /*
         *  Darklist
         */

        if (_kafkaConfig.darklistIsEnabled()) {
            // Create a static state to query the database
            darklistState = topology.newStaticState(new RiakState.Factory<>("rbbi:darklist",
                    _riakConfig.getServers(), 8087, Map.class));

            // Enrich flow stream with darklist fields
            flowStream = flowStream
                    .stateQuery(darklistState, new Fields("flows"), new RiakQuery("src", _debug),
                            new Fields("darklistMap"));

            fieldsFlow.add("darklistMap");
        }

        /*
         *  Join fields and persist
         */

        persist(flowStream.each(new Fields(fieldsFlow), new MergeMapsFunction(_debug), new Fields("finalMap"))
                .project(new Fields("finalMap"))
                .parallelismHint(_config.getWorkers())
                .shuffle().name("Producer"));

        if (_kafkaConfig.contains("events")) {

            persistEvent(eventsStream.each(new Fields(fieldsEvent), new MergeMapsFunction(_debug), new Fields("finalMap"))
                    .project(new Fields("finalMap"))
                    .parallelismHint(_config.getWorkers())
                    .shuffle().name("Event Producer"));
        }

        if (_kafkaConfig.contains("monitor")) {

            persistMonitor(monitorStream.project(new Fields("finalMap"))
                    .parallelismHint(_config.getWorkers())
                    .shuffle().name("Monitor Producer"));
        }

        /*
         *  Show info
         */

        PrintWriter pw = null;

        try {
            pw = new PrintWriter(new FileWriter("/opt/rb/var/redBorder-BI/app/topologyInfo"));
        } catch (IOException e) {
            System.out.println("Error writing info file:" + e);
        }

        print(pw, "----------------------- Topology info: " + "-----------------------");
        print(pw, "- Date topology: " + new Date().toString());
        print(pw, "- Storm workers: " + _config.getWorkers());
        print(pw, "\n- Kafka partitions: ");

        if (locationPartition > 0) print(pw, "   * rb_loc: " + locationPartition);
        if (mobilePartition > 0) print(pw, "   * rb_mobile: " + mobilePartition);
        if (trapPartition > 0) print(pw, "   * rb_trap: " + trapPartition);
        if (flowPartition > 0) print(pw, "   * rb_flow: " + flowPartition);
        if (radiusPartition > 0) print(pw, "   * rb_radius: " + radiusPartition);

        print(pw, "\n- Zookeeper Servers: " + _kafkaConfig.getZkHost("traffics"));
        print(pw, "\n- Riak Servers: " + _riakConfig.getServers().toString());

        if (_outputTopic != null) {
            print(pw, "   * " + _outputTopic + ": " + _config.getKafkaPartitions(_outputTopic));
            print(pw, "Flows send to (kafka topic): " + _outputTopic);
        } else {
            print(pw, "\n- Tranquility info: ");
            print(pw, "   * partitions: " + _tranquilityPartitions);
            print(pw, "   * replicas: " + _tranquilityReplicas);
            print(pw, "\n Flows send to indexing service. \n");
        }

        print(pw, "\n----------------------- Topology Enrichment-----------------------\n");
        print(pw, " - flow: ");
        print(pw, "   * location: " + getEnrichment(_kafkaConfig.contains("location")));
        print(pw, "   * mobile: " + getEnrichment(_kafkaConfig.contains("mobile")));
        print(pw, "   * trap: " + getEnrichment(_kafkaConfig.contains("trap")));
        print(pw, "   * radius (overwrite_cache: " + _kafkaConfig.getOverwriteCache("radius") + ") : " + getEnrichment(_kafkaConfig.contains("radius")));
        print(pw, "   * darklist: " + getEnrichment(_kafkaConfig.darklistIsEnabled()));

        pw.flush();

        return topology;
    }

    private static TridentState persist(Stream s) {
        TridentState ret;

        if (_outputTopic != null) {
            int flowPrePartitions = _config.getKafkaPartitions(_outputTopic);

            ret = s.each(new Fields("finalMap"), new MapToJSONFunction(_debug), new Fields("jsonString"))
                    .partitionPersist(KafkaState.nonTransactional(_kafkaConfig.getZkHost("traffics")),
                            new Fields("jsonString"), new KafkaStateUpdater("jsonString", _outputTopic))
                    .parallelismHint(flowPrePartitions);
        } else {
            StateFactory druidStateFlow = new TridentBeamStateFactory<>(
                    new MyBeamFactoryMapFlow(2, 1, _debug));

            ret = s.partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(_tranquilityPartitions);
        }

        return ret;
    }

    private static TridentState persistEvent(Stream s) {
        TridentState ret;

        if (_outputTopicEvent != null) {
            int eventPrePartitions = _config.getKafkaPartitions(_outputTopicEvent);

            ret = s.each(new Fields("finalMap"), new MapToJSONFunction(_debug), new Fields("jsonString"))
                    .partitionPersist(KafkaState.nonTransactional(_kafkaConfig.getZkHost("events")),
                            new Fields("jsonString"), new KafkaStateUpdater("jsonString", _outputTopicEvent))
                    .parallelismHint(eventPrePartitions);
        } else {
            StateFactory druidStateEvent = new TridentBeamStateFactory<>(
                    new MyBeamFactoryMapEvent(1, 1, _debug));

            ret = s.partitionPersist(druidStateEvent, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(_tranquilityPartitions);
        }

        return ret;
    }

    private static TridentState persistMonitor(Stream s) {
        TridentState ret;

        if (_outputTopicMonitor != null) {
            int monitorPrePartitions = _config.getKafkaPartitions(_outputTopicMonitor);

            ret = s.each(new Fields("finalMap"), new MapToJSONFunction(_debug), new Fields("jsonString"))
                    .partitionPersist(KafkaState.nonTransactional(_kafkaConfig.getZkHost("monitor")),
                            new Fields("jsonString"), new KafkaStateUpdater("jsonString", _outputTopicMonitor))
                    .parallelismHint(monitorPrePartitions);
        } else {
            StateFactory druidStateMonitor = new TridentBeamStateFactory<>(
                    new MyBeamFactoryMapMonitor(1, 1, _debug));

            ret = s.partitionPersist(druidStateMonitor, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(1);
        }

        return ret;
    }

    private static String getEnrichment(boolean bool) {
        return bool ? "✓" : "x";
    }

    private static void print(PrintWriter pw, String msg) {
        if (pw != null) pw.print(msg + "\n");
        System.out.println(msg);
    }

}
