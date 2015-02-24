package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import com.metamx.tranquility.storm.BeamFactory;
import com.metamx.tranquility.storm.TridentBeamStateFactory;
import com.metamx.tranquility.storm.TridentBeamStateUpdater;
import net.redborder.kafkastate.KafkaState;
import net.redborder.kafkastate.KafkaStateUpdater;
import net.redborder.storm.filters.MacLocallyAdministeredFilter;
import net.redborder.storm.function.*;
import net.redborder.storm.siddhi.SiddhiState;
import net.redborder.storm.siddhi.SiddhiUpdater;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.spout.TridentKafkaSpoutNmsp;
import net.redborder.storm.state.CacheNotValidException;
import net.redborder.storm.state.RedBorderState;
import net.redborder.storm.state.StateQuery;
import net.redborder.storm.state.StateUpdater;
import net.redborder.storm.state.gridgain.DarkListQuery;
import net.redborder.storm.util.ConfigData;
import net.redborder.storm.util.druid.BeamEvent;
import net.redborder.storm.util.druid.BeamFlow;
import net.redborder.storm.util.druid.BeamMonitor;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.state.StateFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

/**
 * <p> This is the main class on the project. </p>
 *
 * @author Andres Gomez
 */
public class RedBorderTopology {

    /**
     * Config data has all topology configuration.
     */
    static ConfigData _config;

    /**
     * @param args The value to args can be: {local|cluster} [debug] [force]
     *             <p/>
     *             <p><b>{local|cluster}:</b> You can choose if you want execute the topology on local or on Storm cluster.</p>
     *             <p><b>[debug]:</b> You can active/disable the mode debug.</p>
     *             <p><b>[force]:</b> You force the upload topology to the cluster or local, without it asks you.</p>
     * @throws AlreadyAliveException
     * @throws InvalidTopologyException
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, CacheNotValidException {
        String topologyName = "redBorder-Topology";
        List<String> argsList = new ArrayList<>();

        for (String arg : args) {
            argsList.add(arg);
        }

        if (args.length < 1) {
            System.out.println("./storm jar {name_jar} {main_class} {local|cluster} [debug] [force]");
        } else {
            _config = new ConfigData();

            if (argsList.contains("debug")) {
                _config.debug = true;
            }

            TridentTopology topology = topology();

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = _config.setConfig(args[0]);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, conf, topology.build());
            } else if (args[0].equalsIgnoreCase("cluster")) {
                Config conf = _config.setConfig(args[0]);

                if (argsList.contains("force")) {
                    StormSubmitter.submitTopology(topologyName, conf, topology.build());
                    System.out.println("\nTopology: " + topologyName + " uploaded successfully.");
                } else {
                    System.out.print("Would you like to continue ? (Y/n): ");
                    Scanner sc = new Scanner(System.in);
                    String option = sc.nextLine();
                    if (option.equals("Y") || option.equals("y") || option.equals("") || option.equals("\n")) {
                        StormSubmitter.submitTopology(topologyName, conf, topology.build());
                        System.out.println("\nTopology: " + topologyName + " uploaded successfully.");
                    } else {
                        System.exit(0);
                    }
                }
            }
        }
    }
/*
    public static TridentTopology test() throws CacheNotValidException {
        TridentTopology topology = new TridentTopology();
        topology.newStream("rb_flow", new TridentKafkaSpout(_config, "traffics").builder())
                .each(new Fields("str"), new TwitterSentimentClassifier(), new Fields("sentiment"))
                .each(new Fields("sentiment"), new PrinterFunction("SENTIMENT: "), new Fields("a"));

        return topology;
    }
*/

    /**
     * This method build the redBorder topology based on available sections.
     *
     * @return redBorder Trident Topology
     */
    public static TridentTopology topology() throws CacheNotValidException {
        TridentTopology topology = new TridentTopology();
        List<String> fieldsFlow = new ArrayList<>();
        List<String> fieldsEvent = new ArrayList<>();

        /* States and Streams*/
        TridentState locationState, mobileState, radiusState, trapState, darklistState, nmspState, nmspStateInfo, nmspStateLocationState, locationStasts, locationInfoState;
        StateFactory locationStateFactory, mobileStateFactory, trapStateFactory, radiusStateFactory, nmspStateFactory, nmspStateInfoFactory, nmspStateLocationStateFactory, locationInfoStateFactory;
        Stream locationStream, radiusStream, flowStream = null, eventsStream = null, monitorStream = null, nmspStream, locationStreamUnderV10, locationStreamV10;

        /* Partitions */
        int flowPartition = _config.getKafkaPartitions("rb_flow");
        int eventsPartition = _config.getKafkaPartitions("rb_event");
        int monitorPartition = _config.getKafkaPartitions("rb_monitor");
        int radiusPartition, trapPartition, locationPartition, mobilePartition, nmspPartition;
        radiusPartition = trapPartition = locationPartition = mobilePartition = nmspPartition = 0;

        /* Flow */
        if (_config.contains("traffics")) {
            flowStream = topology.newStream("rb_flow", new TridentKafkaSpout(_config, "traffics").builder())
                    .parallelismHint(flowPartition).shuffle().name("Flows")
                    .each(new Fields("str"), new MapperFunction("rb_flow"), new Fields("flows"))
                    .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"))
                    .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"))
                    .parallelismHint(_config.getWorkers() * _config.getParallelismFactor());
            //.each(new Fields("flows"), new AnalizeHttpUrlFunction(), new Fields("httpUrlMap"));

            fieldsFlow.add("flows");
            fieldsFlow.add("geoIPMap");
            fieldsFlow.add("macVendorMap");
            //fieldsFlow.add("httpUrlMap");
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
            monitorStream = topology.newStream("rb_monitor", new TridentKafkaSpout(_config, "monitor").builder())
                    .parallelismHint(monitorPartition).shuffle().name("Monitor")
                    .each(new Fields("str"), new MapperFunction("rb_monitor"), new Fields("monitorMap"))
                    .each(new Fields("monitorMap"), new CheckTimestampFunction(), new Fields("monitor"));
        }

        /* Trap */
        if (_config.contains("trap")) {
            trapPartition = _config.getKafkaPartitions("rb_trap");
            trapStateFactory = RedBorderState.getStateFactory(_config, "trap");
            trapState = topology.newStaticState(trapStateFactory);

            // Get msg and save it to enrich later on
            Stream rssiStream = topology.newStream("rb_trap", new TridentKafkaSpout(_config, "trap").builder())
                    .name("Trap").parallelismHint(trapPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction("rb_trap"), new Fields("rssi"))
                    .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue", "rssiDruid"))
                    .each(new Fields("rssiDruid"), new MacVendorFunction(), new Fields("rssiMacVendorMap"));


            rssiStream.partitionPersist(trapStateFactory, new Fields("rssiKey", "rssiValue"), StateUpdater.getStateUpdater(_config, "rssiKey", "rssiValue", "trap"));

            if (_config.contains("traffics")) {

                persist("traffics",
                        rssiStream.each(new Fields("rssiDruid", "rssiMacVendorMap"),
                                new MergeMapsFunction(), new Fields("traffics")), "traffics");

                // Enrich flow stream
                flowStream = flowStream
                        .stateQuery(trapState, new Fields("flows"), StateQuery.getStateTrapQuery(_config), new Fields("rssiMap"));

                fieldsFlow.add("rssiMap");
            }
        }

        /* Location */
        if (_config.contains("location")) {
            locationPartition = _config.getKafkaPartitions("rb_loc");

            locationStateFactory = RedBorderState.getStateFactory(_config, "location");
            locationState = topology.newStaticState(locationStateFactory);

            locationInfoStateFactory = RedBorderState.getStateFactory(_config, "location-info");
            locationInfoState = topology.newStaticState(locationInfoStateFactory);

            // Get msg
            locationStream = topology.newStream("rb_loc", new TridentKafkaSpout(_config, "location").builder())
                    .name("Location").parallelismHint(locationPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction("rb_loc"), new Fields("mse_map"))
                    .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data", "mse_data_druid", "mse_version_10"));

            locationStreamUnderV10 = locationStream
                    .each(new Fields("src_mac", "mse_data", "mse_data_druid"), new FilterNull());

            locationStreamV10 = locationStream
                    .project(new Fields("mse_version_10"))
                    .each(new Fields("mse_version_10"), new FilterNull())
                    .each(new Fields("mse_version_10"), new SplitMSE10Data(), new Fields("mse10_association", "mse10_locationUpdate"));


            // Association v10

            Stream associationV10 = locationStreamV10
                    .each(new Fields("mse10_association"), new FilterNull())
                    .each(new Fields("mse10_association"), new ProcessMse10Association(), new Fields("src_mac", "mse10_association_data", "mse10_association_druid"));

            associationV10.partitionPersist(locationInfoStateFactory, new Fields("src_mac", "mse10_association_data"),
                    StateUpdater.getStateUpdater(_config, "src_mac", "mse10_association_data", "location-info"));

            persist("traffics",
                    associationV10, "mse10_association_druid");

            // LocationUpdate v10

            Stream locationUpdateV10 = locationStreamV10
                    .each(new Fields("mse10_locationUpdate"), new FilterNull())
                    .stateQuery(locationInfoState, new Fields("mse10_locationUpdate"), StateQuery.getStateLocationV10Query(_config), new Fields("mseInfoV10"))
                    .each(new Fields("mse10_locationUpdate"), new ProcessMse10LocationUpdate(), new Fields("src_mac", "mse10_locationUpdate_data", "mse10_locationUpdate_druid"));

            locationUpdateV10
                    .each(new Fields("mse10_locationUpdate_data", "mseInfoV10"), new MergeMapsFunction(), new Fields("mse10_data"))
                    .partitionPersist(locationStateFactory, new Fields("src_mac", "mse10_data"), StateUpdater.getStateUpdater(_config, "src_mac", "mse10_data", "location"));

            persist("traffics",
                    locationUpdateV10.each(new Fields("mse10_locationUpdate_druid", "mseInfoV10"), new MergeMapsFunction(), new Fields("mse10_druid")), "mse10_druid");


            if (_config.getMacLocallyAdministeredEnable())
                locationStreamUnderV10 = locationStreamUnderV10.each(new Fields("src_mac"), new MacLocallyAdministeredFilter());

            locationStreamUnderV10 = locationStreamUnderV10.each(new Fields("mse_data_druid"), new MacVendorFunction(), new Fields("mseMacVendorMap"))

                    .each(new Fields("mse_data_druid"), new GeoIpFunction(), new Fields("mseGeoIPMap"));

            if (_config.mseLocationStatsEnabled()) {
                locationStasts = topology.newStaticState(locationStateFactory);

                locationStreamUnderV10 = locationStreamUnderV10.each(new Fields("mse_map"), new GetLocationClient(), new Fields("client"))
                        .stateQuery(locationStasts, new Fields("client"), StateQuery.getStateLocationQuery(_config), new Fields("mseMap"))
                        .each(new Fields("mse_map", "mseMap"), new LocationLogicMse(), new Fields("locationState"));

                persist("location",
                        locationStreamUnderV10, "locationState");
            }

            // Save it to enrich later on
            locationStreamUnderV10.partitionPersist(locationStateFactory, new Fields("src_mac", "mse_data"),
                    StateUpdater.getStateUpdater(_config, "src_mac", "mse_data", "location"));


            if (_config.contains("traffics")) {
                // Generate a flow msg
                persist("traffics",
                        locationStreamUnderV10.each(new Fields("mse_data_druid", "mseMacVendorMap", "mseGeoIPMap"),
                                new MergeMapsFunction(), new Fields("traffics")), "traffics");

                // Enrich flow stream
                flowStream = flowStream.stateQuery(locationState, new Fields("flows"),
                        StateQuery.getStateLocationQuery(_config), new Fields("mseMap"));

                fieldsFlow.add("mseMap");
            }

            if (_config.contains("events")) {
                TridentState locationEventState = topology.newStaticState(locationStateFactory);

                eventsStream = eventsStream
                        .stateQuery(locationEventState, new Fields("event"),
                                StateQuery.getStateEventsLocationMseQuery(_config, "ethsrc", "nmsp"), new Fields("mse_location_ethsrc"))
                        .stateQuery(locationEventState, new Fields("event"),
                                StateQuery.getStateEventsLocationMseQuery(_config, "ethdst", "nmsp"), new Fields("mse_location_ethdst"));

                fieldsEvent.add("mse_location_ethdst");
                fieldsEvent.add("mse_location_ethsrc");
            }
        }

        if (_config.contains("nmsp")) {
            nmspPartition = _config.getKafkaPartitions("rb_nmsp");
            nmspStateFactory = RedBorderState.getStateFactory(_config, "nmsp");
            nmspState = topology.newStaticState(nmspStateFactory);

            nmspStateInfoFactory = RedBorderState.getStateFactory(_config, "nmsp-info");
            nmspStateInfo = topology.newStaticState(nmspStateInfoFactory);

            // Get msg
            nmspStream = topology.newStream("rb_nmsp", new TridentKafkaSpoutNmsp(_config, "nmsp").builder())
                    .name("NMSP").parallelismHint(nmspPartition).shuffle()
                    .each(new Fields("str"), new MapperFunction("rb_nmsp"), new Fields("nsmp_map"))
                    .each(new Fields("nsmp_map"), new GetNMSPdata(), new Fields("nmsp_measure", "nmsp_info"));

            Stream nmspInfoStream = nmspStream.project(new Fields("nmsp_info"))
                    .each(new Fields("nmsp_info"), new GetNMSPInfoData(), new Fields("src_mac", "nmsp_info_data", "nmsp_info_data_druid"));

            if (_config.getMacLocallyAdministeredEnable())
                nmspInfoStream = nmspInfoStream.each(new Fields("src_mac"), new MacLocallyAdministeredFilter());

            nmspInfoStream = nmspInfoStream.each(new Fields("nmsp_info_data_druid"), new PostgreSQLocation(_config.getDbUri(), _config.getDbUser(), _config.getDbPass()), new Fields("infoLocation"))
                    .each(new Fields("nmsp_info_data_druid"), new MacVendorFunction(), new Fields("nmspInfoMacVendorMap"));

            nmspInfoStream.partitionPersist(nmspStateInfoFactory, new Fields("src_mac", "nmsp_info_data"),
                    StateUpdater.getStateUpdater(_config, "src_mac", "nmsp_info_data", "nmsp-info"));

            Stream nmspMeasureStream = nmspStream.project(new Fields("nmsp_measure"))
                    .stateQuery(nmspStateInfo, new Fields("nmsp_measure"), StateQuery.getStateNmspMeasureQuery(_config), new Fields("src_mac", "nmsp_measure_data", "nmsp_measure_data_druid"));

            if (_config.getMacLocallyAdministeredEnable())
                nmspMeasureStream = nmspMeasureStream.each(new Fields("src_mac"), new MacLocallyAdministeredFilter());

            nmspMeasureStream = nmspMeasureStream.each(new Fields("nmsp_measure_data_druid"), new PostgreSQLocation(_config.getDbUri(), _config.getDbUser(), _config.getDbPass()), new Fields("measureLocation"))
                    .each(new Fields("nmsp_measure_data_druid"), new MacVendorFunction(), new Fields("nmspMeasureMacVendorMap"));

            nmspMeasureStream.partitionPersist(nmspStateFactory, new Fields("src_mac", "nmsp_measure_data"),
                    StateUpdater.getStateUpdater(_config, "src_mac", "nmsp_measure_data", "nmsp"));

            if (_config.nmspLocationStatsEnabled()) {
                nmspStateLocationStateFactory = RedBorderState.getStateFactory(_config, "nmsp-location-state");
                nmspStateLocationState = topology.newStaticState(nmspStateLocationStateFactory);

                Stream nmspLocationStateStream = nmspMeasureStream
                        .each(new Fields("nmsp_measure_data_druid", "measureLocation"), new MergeMapsFunction(), new Fields("nmsp_location_state"))
                        .stateQuery(nmspStateLocationState, new Fields("nmsp_location_state"), StateQuery.getStateQuery(_config, "client_mac", "nmsp-state"), new Fields("nmsp_location_state_cache"))
                        .each(new Fields("nmsp_location_state", "nmsp_location_state_cache"), new LocationLogicNmsp(), new Fields("nmsp_location_state_update", "nmsp_location_state_druid"));

                nmspLocationStateStream.partitionPersist(nmspStateLocationStateFactory, new Fields("src_mac", "nmsp_location_state_update"),
                        StateUpdater.getStateUpdater(_config, "src_mac", "nmsp_location_state_update", "nmsp-state"));

                persist("nmsp",
                        nmspLocationStateStream, "nmsp_location_state_druid");
            }

            if (_config.contains("traffics")) {
                // Generate a flow msg

                persist("traffics",
                        nmspMeasureStream.each(new Fields("nmsp_measure_data_druid", "nmspMeasureMacVendorMap", "measureLocation"),
                                new MergeMapsFunction(), new Fields("traffics")).parallelismHint(_config.getWorkers() * _config.getParallelismFactorNmsp()), "traffics");

                persist("traffics",
                        nmspInfoStream.each(new Fields("nmsp_info_data_druid", "nmspInfoMacVendorMap", "infoLocation"),
                                new MergeMapsFunction(), new Fields("traffics")).parallelismHint(_config.getWorkers() * _config.getParallelismFactorNmsp()), "traffics");

                flowStream = flowStream.stateQuery(nmspState, new Fields("flows"),
                        StateQuery.getStateQuery(_config, "client_mac", "nmsp"), new Fields("nmspMap"))
                        .each(new Fields("nmspMap"), new PostgreSQLocation(_config.getDbUri(), _config.getDbUser(), _config.getDbPass()), new Fields("locationWLC"));

                fieldsFlow.add("locationWLC");
                fieldsFlow.add("nmspMap");
            }

            if (_config.contains("events")) {
                eventsStream = eventsStream.stateQuery(nmspState, new Fields("event"),
                        StateQuery.getStateEventsLocationNmspQuery(_config, "ethsrc", "nmsp"), new Fields("nmsp_ap_mac_ethsrc"))
                        .each(new Fields("nmsp_ap_mac_ethsrc"), new PostgreSQLocation(_config.getDbUri(), _config.getDbUser(), _config.getDbPass()), new Fields("locationWLC_ethsrc"))
                        .stateQuery(nmspState, new Fields("event"),
                                StateQuery.getStateEventsLocationNmspQuery(_config, "ethdst", "nmsp"), new Fields("nmsp_ap_mac_ethdst"))
                        .each(new Fields("nmsp_ap_mac_ethdst"), new PostgreSQLocation(_config.getDbUri(), _config.getDbUser(), _config.getDbPass()), new Fields("locationWLC_ethdst"));

                fieldsEvent.add("nmsp_ap_mac_ethdst");
                fieldsEvent.add("locationWLC_ethdst");
                fieldsEvent.add("nmsp_ap_mac_ethsrc");
                fieldsEvent.add("locationWLC_ethsrc");
            }
        }

        /* Mobile */
        if (_config.contains("mobile")) {
            mobilePartition = _config.getKafkaPartitions("rb_mobile");
            mobileStateFactory = RedBorderState.getStateFactory(_config, "mobile");
            mobileState = topology.newStaticState(mobileStateFactory);

            // Get msg and save it to enrich later on
            topology.newStream("rb_mobile", new TridentKafkaSpout(_config, "mobile").builder())
                    .name("Mobile").parallelismHint(mobilePartition).shuffle()
                    .each(new Fields("str"), new MobileBuilderFunction(), new Fields("key", "mobileMap"))
                    .partitionPersist(mobileStateFactory, new Fields("key", "mobileMap"), StateUpdater.getStateUpdater(_config, "key", "mobileMap", "mobile"));

            // Enrich flow stream
            flowStream = flowStream
                    .stateQuery(mobileState, new Fields("flows"), StateQuery.getStateQuery(_config, "src", "mobile"), new Fields("ipAssignMapSrc"))
                    .stateQuery(mobileState, new Fields("flows"), StateQuery.getStateQuery(_config, "dst", "mobile"), new Fields("ipAssignMapDst"))
                    .each(new Fields("ipAssignMapSrc", "ipAssignMapDst"), new MergeMapsFunction(), new Fields("ipAssignMap"))
                    .stateQuery(mobileState, new Fields("ipAssignMap"), StateQuery.getStateQuery(_config, "client_id", "mobile"), new Fields("ueRegisterMap"))
                    .stateQuery(mobileState, new Fields("ueRegisterMap"), StateQuery.getStateQuery(_config, "path", "mobile"), new Fields("hnbRegisterMap"));

            fieldsFlow.add("ipAssignMap");
            fieldsFlow.add("ueRegisterMap");
            fieldsFlow.add("hnbRegisterMap");
        }

        /* Radius */
        if (_config.contains("radius")) {
            radiusPartition = _config.getKafkaPartitions("rb_radius");
            radiusStateFactory = RedBorderState.getStateFactory(_config, "radius");
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
                TridentState radiusStateCache = topology.newStaticState(radiusStateFactory);

                // Get the current radius data from that client and merge it with the data
                // specified on the radius message
                radiusStream = radiusStream
                        .each(new Fields("radius"), new GetRadiusClient(), new Fields("clientMap"))
                        .stateQuery(radiusStateCache, new Fields("clientMap"), StateQuery.getStateQuery(_config, "client_mac", "radius"),
                                new Fields("radiusCached"))
                        .each(new Fields("radius", "radiusCached"), new GetRadiusData(),
                                new Fields("radiusKey", "radiusData", "radiusDruid"));
            }

            // Save msg to enrich later on
            radiusStream.project(new Fields("radiusKey", "radiusData"))
                    .partitionPersist(radiusStateFactory, new Fields("radiusKey", "radiusData"),
                            StateUpdater.getStateUpdater(_config, "radiusKey", "radiusData", "radius"));

            // Generate a flow msg
            persist("traffics",
                    radiusStream
                            .each(new Fields("radiusDruid"), new MacVendorFunction(), new Fields("radiusMacVendorMap"))
                            .each(new Fields("radiusDruid"), new GeoIpFunction(), new Fields("radiusGeoIPMap"))
                            .each(new Fields("radiusDruid", "radiusMacVendorMap", "radiusGeoIPMap"),
                                    new MergeMapsFunction(), new Fields("traffics")), "traffics");

            // Enrich flow stream
            flowStream = flowStream.stateQuery(radiusState, new Fields("flows"), StateQuery.getStateQuery(_config, "client_mac", "radius"),
                    new Fields("radiusMap"));

            fieldsFlow.add("radiusMap");
        }

        /* Darklist */
        if (_config.darklistIsEnabled() && _config.getCacheType().equals("gridgain")) {
            // Create a static state to query the database
            darklistState = topology.newStaticState(RedBorderState.getStateFactory(_config, "darklist"));

            // Enrich flow stream with darklist fields
            if (_config.contains("traffics")) {
                flowStream = flowStream
                        .stateQuery(darklistState, new Fields("flows"), new DarkListQuery(_config.darklistType()),
                                new Fields("darklistMap"));

                fieldsFlow.add("darklistMap");
            }


            // Enrich event stream with darklist fields
            if (_config.contains("event")) {
                eventsStream = eventsStream
                        .stateQuery(darklistState, new Fields("event"), new DarkListQuery(_config.darklistType()),
                                new Fields("darklistMap"));

                fieldsEvent.add("darklistMap");
            }

        }

        List<Stream> mainStream = new ArrayList<>();
        /* Join fields and persist */
        if (_config.contains("traffics")) {

            flowStream = flowStream.each(new Fields(fieldsFlow), new MergeMapsFunction(fieldsFlow), new Fields("mergedMap"))
                    .each(new Fields("mergedMap"), new SeparateLongTimeFlowFunction(), new Fields("separateTime"))
                    .each(new Fields("separateTime"), new CheckTimestampFunction(), new Fields("traffics"));

            if (_config.getCorrealtionEnabled()) {

                flowStream = flowStream.each(new Fields("traffics"), new AddSection("traffics"), new Fields("section"))
                        .project(new Fields("section", "traffics"));
            }

            persist("traffics", flowStream
                    .project(new Fields("traffics"))
                    .parallelismHint(_config.getWorkers() * _config.getParallelismFactor())
                    .shuffle().name("Flow Producer"), "traffics");

            mainStream.add(flowStream);
        }

        if (_config.contains("events")) {

            eventsStream = eventsStream.each(new Fields(fieldsEvent), new MergeMapsFunction(fieldsEvent), new Fields("mergedMap"))
                    .each(new Fields("mergedMap"), new CheckTimestampFunction(), new Fields("events"));

            if (_config.getCorrealtionEnabled()) {
                eventsStream = eventsStream.each(new Fields("events"), new AddSection("events"), new Fields("section"))
                        .project(new Fields("section", "events"));
            }

            persist("events",
                    eventsStream
                            .project(new Fields("events"))
                            .parallelismHint(eventsPartition)
                            .shuffle().name("Event Producer"), "events");

            mainStream.add(eventsStream);
        }

        if (_config.contains("monitor")) {

            if (_config.getCorrealtionEnabled()) {
                monitorStream = monitorStream.each(new Fields("monitor"), new AddSection("monitor"), new Fields("section"))
                        .project(new Fields("section", "monitor"));
            }

            persist("monitor",
                    monitorStream.project(new Fields("monitor"))
                            .parallelismHint(monitorPartition)
                            .shuffle().name("Monitor Producer"), "monitor");

            mainStream.add(monitorStream);
        }

        if (_config.getCorrealtionEnabled()) {
            topology.merge(new Fields("sections", "maps"), mainStream)
                    .partitionPersist(SiddhiState.nonTransactional(_config.getZkHost()), new Fields("sections", "maps"), new SiddhiUpdater())
                    .parallelismHint(1);
        }


        /* Show info */
        PrintWriter pw = null;

        try {
            pw = new PrintWriter(new FileWriter("/opt/rb/var/rb-bi/app/topologyInfo"));
        } catch (IOException e) {
            System.out.println("Error writing info file:" + e);
        }

        print(pw, "----------------------- Topology info: -----------------------");
        print(pw, "- Debug: " + (_config.debug ? "ON" : "OFF"));
        print(pw, "- Date topology: " + new Date().toString());
        print(pw, "- Storm workers: " + _config.getWorkers());
        print(pw, "- Kafka partitions: ");

        if (_config.contains("location") && locationPartition > 0) print(pw, "   * rb_loc: " + locationPartition);
        if (_config.contains("mobile") && mobilePartition > 0) print(pw, "   * rb_mobile: " + mobilePartition);
        if (_config.contains("trap") && trapPartition > 0) print(pw, "   * rb_trap: " + trapPartition);
        if (_config.contains("traffics") && flowPartition > 0) print(pw, "   * rb_flow: " + flowPartition);
        if (_config.contains("radius") && radiusPartition > 0) print(pw, "   * rb_radius: " + radiusPartition);
        if (_config.contains("radius") && nmspPartition > 0) print(pw, "   * rb_nmsp: " + nmspPartition);


        print(pw, "- Zookeeper Servers: " + _config.getZkHost());

        print(pw, "- Cache Backend: " + _config.getCacheType());


        List<String> servers = null;

        if (_config.getCacheType().equals("gridgain")) {
            servers = _config.getGridGainServers();
            print(pw, "   - multicast: " + _config.getGridGainMulticast());
        } else if (_config.getCacheType().equals("riak")) {
            servers = _config.getRiakServers();
        } else if (_config.getCacheType().equals("memcached")) {
            servers = _config.getMemcachedServersAsString();
        }

        print(pw, "   - servers: " + servers);
        print(pw, "- Correlation Engine: " + _config.getCorrealtionEnabled());


        print(pw, "\n----------------------- Topology Enrichment -----------------------");

        if (_config.contains("traffics")) {
            print(pw, " - flow: ");
            print(pw, "   * nmsp: " + getEnrichment(_config.contains("nmsp")) + " (" + "location_stats: " + getEnrichment(_config.nmspLocationStatsEnabled()) + ")");
            print(pw, "   * location: " + getEnrichment(_config.contains("location")) + "   (location state: " + _config.mseLocationStatsEnabled() + ")");
            print(pw, "   * mobile: " + getEnrichment(_config.contains("mobile")));
            print(pw, "   * trap: " + getEnrichment(_config.contains("trap")));
            print(pw, "   * radius (overwrite_cache: " + _config.getOverwriteCache("radius") + ") : " + getEnrichment(_config.contains("radius")));
            print(pw, "   * darklist: " + getEnrichment(_config.darklistIsEnabled()));

            if (_config.tranquilityEnabled("traffics")) {
                print(pw, "   * output to tranquility: ");
                print(pw, "     * partitions: " + _config.tranquilityPartitions("traffics"));
                print(pw, "     * replicas: " + _config.tranquilityReplication());
            } else {
                String output = _config.getOutputTopic("traffics");
                print(pw, "   * output topic: " + output + " (partitions: " + _config.getKafkaPartitions(output) + ")");
                if (_config.mseLocationStatsEnabled()) {
                    output = _config.getOutputTopic("location");
                    print(pw, "   * output topic: " + output + " (partitions: " + _config.getKafkaPartitions(output) + ")");
                }
            }
        }

        if (_config.contains("events")) {
            print(pw, " - event: ");
            print(pw, "   * darklist: " + getEnrichment(_config.darklistIsEnabled()));

            if (_config.tranquilityEnabled("events")) {
                print(pw, "   * output to tranquility: ");
                print(pw, "     * partitions: " + _config.tranquilityPartitions("events"));
                print(pw, "     * replicas: " + _config.tranquilityReplication());
            } else {
                String output = _config.getOutputTopic("events");
                print(pw, "   * output topic: " + output + " (partitions: " + _config.getKafkaPartitions(output) + ")");
            }
        }

        if (_config.contains("monitor")) {
            print(pw, " - monitor ");

            if (_config.tranquilityEnabled("monitor")) {
                print(pw, "   * output to tranquility: ");
                print(pw, "     * partitions: " + _config.tranquilityPartitions("monitor"));
                print(pw, "     * replicas: " + _config.tranquilityReplication());
            } else {
                String output = _config.getOutputTopic("monitor");
                print(pw, "   * output topic: " + output + " (partitions:" + _config.getKafkaPartitions(output) + ")");
            }
        }


        print(pw, "\n----------------------- Topology Metrics -----------------------");
        print(pw, " - KafkaOffsetsConsumerMonitor: " + getEnrichment(true));
        print(pw, " - Metrics2KafkaProducer: ");
        print(pw, "   * Throughput: " + getEnrichment(_config.getMetrics()));
        print(pw, "\n----------------------------------------------------------------");

        if (_config.darklistIsEnabled() && _config.getCacheType().equals("gridgain"))
            print(pw, "[WARN] You need use gridgain backend to can use darklist.");

        pw.flush();

        return topology;
    }

    private static TridentState persist(String topic, Stream s, String field) {
        String outputTopic = _config.getOutputTopic(topic);
        int partitions = _config.tranquilityPartitions(topic);
        int replication = _config.tranquilityReplication();
        TridentState ret;

        if (outputTopic != null) {
            int flowPrePartitions = _config.getKafkaPartitions(outputTopic);

            ret = s.each(new Fields(field), new MapToJSONFunction(), new Fields("jsonString"))
                    .partitionPersist(KafkaState.nonTransactional(_config.getZkHost()),
                            new Fields("jsonString"), new KafkaStateUpdater("jsonString", outputTopic))
                    .parallelismHint(flowPrePartitions);
        } else {
            BeamFactory bf;
            TridentBeamStateFactory druidState = null;
            String zkHost = _config.getZkHost();

            switch (topic) {
                case "traffics":
                    bf = new BeamFlow(partitions, replication, zkHost, _config.getMaxRows());
                    druidState = new TridentBeamStateFactory<>(bf);
                    break;
                case "events":
                    bf = new BeamEvent(partitions, replication, zkHost, _config.getMaxRows());
                    druidState = new TridentBeamStateFactory<>(bf);
                    break;
                case "monitor":

                    bf = new BeamMonitor(partitions, replication, zkHost, _config.getMaxRows());
                    druidState = new TridentBeamStateFactory<>(bf);
                    break;
                default:
                    System.out.println("Tranquility beams not defined!");
                    break;
            }

            ret = s.partitionPersist(druidState, new Fields(field), new TridentBeamStateUpdater())
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
