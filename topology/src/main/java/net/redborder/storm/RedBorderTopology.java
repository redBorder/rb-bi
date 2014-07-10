package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import com.github.quintona.KafkaState;
import com.github.quintona.KafkaStateUpdater;
import com.metamx.tranquility.storm.BeamFactory;
import com.metamx.tranquility.storm.TridentBeamStateFactory;
import com.metamx.tranquility.storm.TridentBeamStateUpdater;
import net.redborder.state.gridgain.GridGainFactory;
import net.redborder.storm.function.*;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.state.DarkListQuery;
import net.redborder.storm.state.LocationQuery;
import net.redborder.storm.state.StateQuery;
import net.redborder.storm.state.StateUpdater;
import net.redborder.storm.util.ConfigData;
import net.redborder.storm.util.druid.BeamEvent;
import net.redborder.storm.util.druid.BeamFlow;
import net.redborder.storm.util.druid.BeamMonitor;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RedBorderTopology {

    static ConfigData _config;

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String topologyName = "redBorder-Topology";

        if (args.length < 1) {
            System.out.println("./storm jar {name_jar} {main_class} {local|cluster} [debug]");
        } else {
            _config = new ConfigData();

            if (args.length == 2) {
                if (args[1].equals("debug")) {
                    _config.debug = true;
                } else {
                    System.out.println("./storm jar {name_jar} {main_class} {local|cluster} [debug]");
                }
            }

            TridentTopology topology = topology();

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = _config.setConfig(args[0]);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, conf, topology.build());
            } else if (args[0].equalsIgnoreCase("cluster")) {
                Config conf = _config.setConfig(args[0]);
                StormSubmitter.submitTopology(topologyName, conf, topology.build());
                System.out.println("\nTopology: " + topologyName + " uploaded successfully.");
            }
        }
    }

    public static TridentTopology topology() {
        TridentTopology topology = new TridentTopology();
        List<String> fieldsFlow = new ArrayList<>();
        List<String> fieldsEvent = new ArrayList<>();

        /* States and Streams*/
        TridentState locationState, mobileState, radiusState, trapState, darklistState;
        GridGainFactory locationStateFactory, mobileStateFactory, trapStateFactory, radiusStateFactory;
        Stream locationStream, radiusStream, flowStream = null, eventsStream = null, monitorStream = null;

        /* Partitions */
        int flowPartition = _config.getKafkaPartitions("rb_flow");
        int eventsPartition = _config.getKafkaPartitions("rb_event");
        int monitorPartition = _config.getKafkaPartitions("rb_monitor");
        int radiusPartition, trapPartition, locationPartition, mobilePartition;
        radiusPartition = trapPartition = locationPartition = mobilePartition = 0;

        /* Flow */
        if (_config.contains("traffics")) {
             flowStream = topology.newStream("rb_flow", new TridentKafkaSpout(_config, "traffics").builder())
                    .parallelismHint(flowPartition).shuffle().name("Flows")
                    .each(new Fields("str"), new MapperFunction("rb_flow"), new Fields("flows"))
                    .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"))
                    .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"))
                    .each(new Fields("flows"), new AnalizeHttpUrlFunction(), new Fields("httpUrlMap"));

            fieldsFlow.add("flows");
            fieldsFlow.add("geoIPMap");
            fieldsFlow.add("macVendorMap");
            fieldsFlow.add("httpUrlMap");
        }

        /* Events */
        if (_config.contains("events")) {
            eventsStream = topology.newStream("rb_event", new TridentKafkaSpout(_config, "events").builder())
                    .parallelismHint(eventsPartition).shuffle().name("Events")
                    .each(new Fields("str"), new MapperFunction("rb_event"), new Fields("event"))
                    .each(new Fields("event"), new MacVendorFunction(), new Fields("macVendorMap"))
                    .each(new Fields("event"), new GeoIpFunction(), new Fields("geoIPMap"));

            fieldsEvent.add("event");
            fieldsEvent.add("macVendorMap");
            fieldsEvent.add("geoIPMap");
        }

        /* Monitor */
        if (_config.contains("monitor")) {
            locationPartition = _config.getKafkaPartitions("rb_monitor");

            monitorStream = topology.newStream("rb_monitor", new TridentKafkaSpout(_config, "monitor").builder())
                    .parallelismHint(monitorPartition).shuffle().name("Monitor")
                    .each(new Fields("str"), new MapperFunction("rb_monitor"), new Fields("finalMap"));
        }

        /* Location */
        if (_config.contains("location")) {
            locationPartition = _config.getKafkaPartitions("rb_loc");
            locationStateFactory = new GridGainFactory("location", _config.getEnrichs());
            locationState = topology.newStaticState(locationStateFactory);

            // Get msg
            locationStream = topology.newStream("rb_loc", new TridentKafkaSpout(_config, "location").builder())
                    .name("Location").parallelismHint(locationPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction("rb_loc"), new Fields("mse_map"))
                    .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data", "mse_data_druid"))
                    .each(new Fields("mse_data_druid"), new MacVendorFunction(), new Fields("mseMacVendorMap"))
                    .each(new Fields("mse_data_druid"), new GeoIpFunction(), new Fields("mseGeoIPMap"));

            // Save it to enrich later on
            locationStream.partitionPersist(locationStateFactory, new Fields("src_mac", "mse_data"),
                    new StateUpdater("src_mac", "mse_data"));

            if(_config.contains("traffics")) {
                // Generate a flow msg
                persist("traffics",
                        locationStream.each(new Fields("mse_data_druid", "mseMacVendorMap", "mseGeoIPMap"),
                                new MergeMapsFunction(), new Fields("finalMap")));

                // Enrich flow stream
                flowStream = flowStream.stateQuery(locationState, new Fields("flows"),
                        new LocationQuery("client_mac"), new Fields("mseMap"));

                fieldsFlow.add("mseMap");
            }
        }

        /* Mobile */
        if (_config.contains("mobile")) {
            mobilePartition = _config.getKafkaPartitions("rb_mobile");
            mobileStateFactory = new GridGainFactory("mobile", _config.getEnrichs());
            mobileState = topology.newStaticState(mobileStateFactory);

            // Get msg and save it to enrich later on
            topology.newStream("rb_mobile", new TridentKafkaSpout(_config, "mobile").builder())
                    .name("Mobile").parallelismHint(mobilePartition).shuffle()
                    .each(new Fields("str"), new MobileBuilderFunction(), new Fields("key", "mobileMap"))
                    .partitionPersist(mobileStateFactory, new Fields("key", "mobileMap"), new StateUpdater("key", "mobileMap"));

            // Enrich flow stream
            flowStream = flowStream
                    .stateQuery(mobileState, new Fields("flows"), new StateQuery("src"), new Fields("ipAssignMap"))
                    .stateQuery(mobileState, new Fields("ipAssignMap"), new StateQuery("client_id"), new Fields("ueRegisterMap"))
                    .stateQuery(mobileState, new Fields("ueRegisterMap"), new StateQuery("path"), new Fields("hnbRegisterMap"));

            fieldsFlow.add("ipAssignMap");
            fieldsFlow.add("ueRegisterMap");
            fieldsFlow.add("hnbRegisterMap");
        }

        /* Trap */
        if (_config.contains("trap")) {
            trapPartition = _config.getKafkaPartitions("rb_trap");
            trapStateFactory = new GridGainFactory("trap", _config.getEnrichs());
            trapState = topology.newStaticState(trapStateFactory);

            // Get msg and save it to enrich later on
            topology.newStream("rb_trap", new TridentKafkaSpout(_config, "trap").builder())
                    .name("Trap").parallelismHint(trapPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction("rb_trap"), new Fields("rssi"))
                    .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"))
                    .partitionPersist(trapStateFactory, new Fields("rssiKey", "rssiValue"), new StateUpdater("rssiKey", "rssiValue"));

            // Enrich flow stream
            flowStream = flowStream
                    .stateQuery(trapState, new Fields("flows"), new StateQuery("client_mac"), new Fields("rssiMap"));

            fieldsFlow.add("rssiMap");
        }

        /* Radius */
        if (_config.contains("radius")) {
            radiusPartition = _config.getKafkaPartitions("rb_radius");
            radiusStateFactory = new GridGainFactory("radius", _config.getEnrichs());
            radiusState = topology.newStaticState(radiusStateFactory);

            // Get msg
            radiusStream = topology.newStream("rb_radius", new TridentKafkaSpout(_config, "radius").builder())
                    .name("Radius").parallelismHint(radiusPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction("rb_radius"), new Fields("radius"));

            if (_config.getOverwriteCache("radius")) {
                // Get the radius data from the radius message
                radiusStream = radiusStream
                        .each(new Fields("radius"), new GetRadiusData(),
                                new Fields("radiusKey", "radiusData", "radiusDruid"));

            } else {
                // Get the current radius data from that client and merge it with the data
                // specified on the radius message
                radiusStream = radiusStream
                        .each(new Fields("radius"), new GetRadiusClient(), new Fields("clientMap"))
                        .stateQuery(radiusState, new Fields("clientMap"), new StateQuery("client_mac"),
                                new Fields("radiusCached"))
                        .each(new Fields("radius", "radiusCached"), new GetRadiusData(),
                                new Fields("radiusKey", "radiusData", "radiusDruid"));
            }

            // Save msg to enrich later on
            radiusStream.project(new Fields("radiusKey", "radiusData"))
                    .partitionPersist(radiusStateFactory, new Fields("radiusKey", "radiusData"),
                            new StateUpdater("radiusKey", "radiusData"));

            // Generate a flow msg
            persist("traffics",
                    radiusStream
                            .each(new Fields("radiusDruid"), new MacVendorFunction(), new Fields("radiusMacVendorMap"))
                            .each(new Fields("radiusDruid"), new GeoIpFunction(), new Fields("radiusGeoIPMap"))
                            .each(new Fields("radiusDruid", "radiusMacVendorMap", "radiusGeoIPMap"),
                                    new MergeMapsFunction(), new Fields("finalMap")));

            // Enrich flow stream
            flowStream = flowStream.stateQuery(radiusState, new Fields("flows"), new StateQuery("client_mac"),
                    new Fields("radiusMap"));

            fieldsFlow.add("radiusMap");
        }

        /* Darklist */
        if (_config.darklistIsEnabled()) {
            // Create a static state to query the database
            darklistState = topology.newStaticState(new GridGainFactory("darklist", _config.getEnrichs()));

            // Enrich flow stream with darklist fields
            if(_config.contains("traffics")) {
                flowStream = flowStream
                        .stateQuery(darklistState, new Fields("flows"), new DarkListQuery(),
                                new Fields("darklistMap"));

                fieldsFlow.add("darklistMap");
            }


            // Enrich event stream with darklist fields
            if(_config.contains("events")){
                eventsStream = eventsStream
                        .stateQuery(darklistState, new Fields("event"), new DarkListQuery(),
                                new Fields("darklistMap"));

                fieldsEvent.add("darklistMap");
            }

        }

        /* Join fields and persist */
        if (_config.contains("traffics")) {
            persist("traffics",
                    flowStream.each(new Fields(fieldsFlow), new MergeMapsFunction(), new Fields("mergedMap"))
                            .each(new Fields("mergedMap"), new SeparateLongTimeFlowFunction(), new Fields("finalMap"))
                            .project(new Fields("finalMap"))
                            .parallelismHint(_config.getWorkers())
                            .shuffle().name("Flow Producer"));
        }

        if (_config.contains("events")) {
            persist("events",
                    eventsStream.each(new Fields(fieldsEvent), new MergeMapsFunction(), new Fields("finalMap"))
                            .project(new Fields("finalMap"))
                            .parallelismHint(_config.getWorkers())
                            .shuffle().name("Event Producer"));
        }

        if (_config.contains("monitor")) {
            persist("monitor",
                    monitorStream.project(new Fields("finalMap"))
                            .parallelismHint(_config.getWorkers())
                            .shuffle().name("Monitor Producer"));
        }

        /* Show info */
        PrintWriter pw = null;

        try {
            pw = new PrintWriter(new FileWriter("/opt/rb/var/redBorder-BI/app/topologyInfo"));
        } catch (IOException e) {
            System.out.println("Error writing info file:" + e);
        }

        print(pw, "----------------------- Topology info: -----------------------");
        print(pw, "- Debug: " + (_config.debug ? "ON" : "OFF"));
        print(pw, "- Date topology: " + new Date().toString());
        print(pw, "- Storm workers: " + _config.getWorkers());
        print(pw, "- Kafka partitions: ");

        if (locationPartition > 0) print(pw, "   * rb_loc: " + locationPartition);
        if (mobilePartition > 0) print(pw, "   * rb_mobile: " + mobilePartition);
        if (trapPartition > 0) print(pw, "   * rb_trap: " + trapPartition);
        if (flowPartition > 0) print(pw, "   * rb_flow: " + flowPartition);
        if (radiusPartition > 0) print(pw, "   * rb_radius: " + radiusPartition);

        print(pw, "- Zookeeper Servers: " + _config.getZkHost());

        if (_config.tranquilityEnabled("traffics")) {
            print(pw, "- Tranquility info: ");
            print(pw, "   * partitions: " + _config.tranquilityPartitions("traffics"));
            print(pw, "   * replicas: " + _config.tranquilityReplication());
            print(pw, " Flows send to indexing service.");
        } else {
            String output = _config.getOutputTopic("traffics");
            print(pw, "   * " + output + ": " + _config.getKafkaPartitions(output));
            print(pw, "Flows send to (kafka topic): " + output);
        }

        print(pw, "\n----------------------- Topology Enrichment -----------------------");
        print(pw, " - flow: ");
        print(pw, "   * location: " + getEnrichment(_config.contains("location")));
        print(pw, "   * mobile: " + getEnrichment(_config.contains("mobile")));
        print(pw, "   * trap: " + getEnrichment(_config.contains("trap")));
        print(pw, "   * radius (overwrite_cache: " + _config.getOverwriteCache("radius") + ") : " + getEnrichment(_config.contains("radius")));
        print(pw, "   * darklist: " + getEnrichment(_config.darklistIsEnabled()));


        print(pw, "\n----------------------- Topology Metrics -----------------------");
        print(pw, " - KafkaOffsetsConsumerMonitor: " + getEnrichment(true));
        print(pw, " - Metrics2KafkaConsumer: ");
        print(pw, "   * Throughput: " + getEnrichment(_config.getMetrics()) + "\n");


        pw.flush();

        return topology;
    }

    private static TridentState persist(String topic, Stream s) {
        String outputTopic = _config.getOutputTopic(topic);
        int partitions = _config.tranquilityPartitions(topic);
        int replication = _config.tranquilityReplication();
        TridentState ret;

        if (outputTopic != null) {
            int flowPrePartitions = _config.getKafkaPartitions(outputTopic);

            ret = s.each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"))
                    .partitionPersist(KafkaState.nonTransactional(_config.getZkHost()),
                            new Fields("jsonString"), new KafkaStateUpdater("jsonString", outputTopic))
                    .parallelismHint(flowPrePartitions);
        } else {
            BeamFactory bf;
            TridentBeamStateFactory druidState;
            String zkHost = _config.getZkHost();

            if (topic.equals("traffics")) {
                bf = new BeamFlow(partitions, replication, zkHost);
                druidState = new TridentBeamStateFactory<BeamFlow>(bf);
            } else if (topic.equals("events")) {
                bf = new BeamEvent(partitions, replication, zkHost);
                druidState = new TridentBeamStateFactory<BeamEvent>(bf);
            } else {
                bf = new BeamMonitor(partitions, replication, zkHost);
                druidState = new TridentBeamStateFactory<BeamMonitor>(bf);
            }

            ret = s.partitionPersist(druidState, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(partitions);
        }

        return ret;
    }

    private static String getEnrichment(boolean bool) {
        return bool ? "yes" : "no";
    }

    private static void print(PrintWriter pw, String msg) {
        if (pw != null) pw.print(msg + "\n");
        System.out.println(msg);
    }

}
