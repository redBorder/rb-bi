/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.topologies;

import backtype.storm.tuple.Fields;
import com.metamx.tranquility.storm.TridentBeamStateFactory;
import com.metamx.tranquility.storm.TridentBeamStateUpdater;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import net.redborder.storm.function.*;
import net.redborder.storm.spout.*;
import net.redborder.storm.state.*;
import net.redborder.storm.state.query.*;
import net.redborder.storm.state.updater.*;
import net.redborder.storm.util.*;
import net.redborder.storm.util.druid.*;
import nl.minvenj.nfi.storm.kafka.util.KafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

/**
 *
 * @author andresgomez
 */
public class RedBorderTopologies {

    public TridentTopology newKafka() throws FileNotFoundException {
        KafkaConfigFile kafkaConfig = new KafkaConfigFile();
        KafkaConfig configConsumer = new KafkaConfig();
        MemcachedConfigFile memConfig = new MemcachedConfigFile();
        TridentTopology topology = new TridentTopology();
        MemcachedState.Options mseOpts = new MemcachedState.Options();
        mseOpts.expiration = 360000;
        MemcachedState.Options mobileOpts = new MemcachedState.Options();
        mobileOpts.expiration = 0;

        StateFactory memcached = MemcachedState.transactional(memConfig.getServers(), mseOpts);
        StateFactory memcachedMobile = MemcachedState.transactional(memConfig.getServers(), mobileOpts);

        // LOCATION DATA
        Stream mseStream = topology.newStream("rb_loc", new TridentKafkaSpoutNew(kafkaConfig, "location").builder(configConsumer))
                .parallelismHint(2)
                .shuffle()
                .each(new Fields("bytes"), new MapperFunction(), new Fields("mse_map"))
                .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data", "mse_data_druid"));

        TridentState mseState = mseStream
                .project(new Fields("src_mac", "mse_data"))
                .partitionPersist(memcached, new Fields("src_mac", "mse_data"), new MemcachedUpdater("src_mac", "mse_data", "rb_loc"));

        // MOBILE DATA
        TridentState mobileState = topology.newStream("rb_mobile", new TridentKafkaSpoutNew(kafkaConfig, "mobile").builder(configConsumer))
                .parallelismHint(2)
                .shuffle()
                .each(new Fields("bytes"), new MobileBuilderFunction(), new Fields("key", "mobileMap"))
                .partitionPersist(memcachedMobile, new Fields("key", "mobileMap"), new MemcachedUpdater("key", "mobileMap", "rb_mobile"));

        // RSSI DATA
        TridentState rssiState = topology.newStream("rb_trap", new TridentKafkaSpoutNew(kafkaConfig, "trap").builder(configConsumer))
                .parallelismHint(2)
                .shuffle()
                .each(new Fields("bytes"), new MapperFunction(), new Fields("rssi"))
                .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"))
                .partitionPersist(memcached, new Fields("rssiKey", "rssiValue"), new MemcachedUpdater("rssiKey", "rssiValue", "rb_trap"));

        // FLOW STREAM
        Stream flowStream = topology.newStream("rb_flow", new TridentKafkaSpoutNew(kafkaConfig, "traffics").builder(configConsumer))
                .parallelismHint(2)
                .shuffle()
                .each(new Fields("bytes"), new MapperFunction(), new Fields("flows"))
                .project(new Fields("flows"));

        Stream locationStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow", "rb_loc"), new Fields("mseMap"))
                .project(new Fields("flows", "mseMap"));

        Stream trapStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(rssiState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow", "rb_trap"), new Fields("rssiMap"))
                .project(new Fields("flows", "rssiMap"));

        Stream macVendorStream = flowStream
                .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"));

        Stream geoIPStream = flowStream
                .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"));

        Stream httpUrlStream = flowStream
                .each(new Fields("flows"), new AnalizeHttpUrlFunction(), new Fields("httpUrlMap"));

        Stream mobileStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("src"), new Fields("src_ip_addr"))
                .stateQuery(mobileState, new Fields("src_ip_addr"), new MemcachedQuery("src_ip_addr", "rb_mobile"), new Fields("ipAssignMap"))
                .each(new Fields("ipAssignMap"), new GetFieldFunction("imsi"), new Fields("imsi"))
                .stateQuery(mobileState, new Fields("imsi"), new MemcachedQuery("imsi", "rb_mobile"), new Fields("ueRegisterMap"))
                .each(new Fields("ueRegisterMap"), new GetFieldFunction("path"), new Fields("path"))
                .stateQuery(mobileState, new Fields("path"), new MemcachedQuery("path", "rb_mobile"), new Fields("hnbRegisterMap"))
                .each(new Fields("ipAssignMap", "ueRegisterMap", "hnbRegisterMap"), new JoinFlowFunction(), new Fields("mobileMap"))
                .project(new Fields("flows", "mobileMap"));

        List<Stream> joinStream = new ArrayList<>();
        joinStream.add(locationStream);
        joinStream.add(macVendorStream);
        joinStream.add(geoIPStream);
        joinStream.add(mobileStream);
        joinStream.add(trapStream);
        joinStream.add(httpUrlStream);

        List<Fields> keyFields = new ArrayList<>();
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));

        Stream joinedStream = topology.join(joinStream, keyFields, new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap", "rssiMap", "httpUrlMap"))
                .each(new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap", "rssiMap", "httpUrlMap"), new JoinFlowFunction(), new Fields("finalMap"));

        String outputTopic = kafkaConfig.getOutputTopic();
        if (outputTopic != null) {
            joinedStream.each(new Fields("finalMap"), new SeparateLongTimeFlowFunction(), new Fields("separatedMap"))
                    .each(new Fields("separatedMap"), new MapToJSONFunction(), new Fields("jsonString"))
                    .each(new Fields("jsonString"), new ProducerKafkaFunction(kafkaConfig, outputTopic), new Fields("a"))
                    .parallelismHint(4);

            mseStream
                    .each(new Fields("mse_data_druid"), new MapToJSONFunction(), new Fields("jsonString"))
                    .each(new Fields("jsonString"), new ProducerKafkaFunction(kafkaConfig, outputTopic), new Fields("a"));
        } else {
            StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow());

            joinedStream.each(new Fields("finalMap"), new SeparateLongTimeFlowFunction(), new Fields("separatedMap"))
                    .partitionPersist(druidStateFlow, new Fields("separatedMap"), new TridentBeamStateUpdater())
                    .parallelismHint(4);

            mseStream
                    .project(new Fields("mse_data_druid"))
                    .partitionPersist(druidStateFlow, new Fields("mse_data_druid"), new TridentBeamStateUpdater());
        }

        return topology;
    }

    public TridentTopology all() throws FileNotFoundException {
        KafkaConfigFile kafkaConfig = new KafkaConfigFile();
        MemcachedConfigFile memConfig = new MemcachedConfigFile();
        TridentTopology topology = new TridentTopology();
        MemcachedState.Options mseOpts = new MemcachedState.Options();
        mseOpts.expiration = 360000;
        MemcachedState.Options mobileOpts = new MemcachedState.Options();
        mobileOpts.expiration = 0;

        StateFactory memcached = MemcachedState.transactional(memConfig.getServers(), mseOpts);
        StateFactory memcachedMobile = MemcachedState.transactional(memConfig.getServers(), mobileOpts);

        // LOCATION DATA
        Stream mseStream = topology.newStream("rb_loc", new TridentKafkaSpout(kafkaConfig, "location").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("mse_map"))
                .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data", "mse_data_druid"));

        TridentState mseState = mseStream
                .project(new Fields("src_mac", "mse_data"))
                .partitionPersist(memcached, new Fields("src_mac", "mse_data"), new MemcachedUpdater("src_mac", "mse_data", "rb_loc"));

        // MOBILE DATA
        TridentState mobileState = topology.newStream("rb_mobile", new TridentKafkaSpout(kafkaConfig, "mobile").builder())
                .each(new Fields("str"), new MobileBuilderFunction(), new Fields("key", "mobileMap"))
                .partitionPersist(memcachedMobile, new Fields("key", "mobileMap"), new MemcachedUpdater("key", "mobileMap", "rb_mobile"));

        // RSSI DATA
        TridentState rssiState = topology.newStream("rb_trap", new TridentKafkaSpout(kafkaConfig, "trap").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("rssi"))
                .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"))
                .partitionPersist(memcached, new Fields("rssiKey", "rssiValue"), new MemcachedUpdater("rssiKey", "rssiValue", "rb_trap"));

        // FLOW STREAM
        Stream flowStream = topology.newStream("rb_flow", new TridentKafkaSpout(kafkaConfig, "traffics").builder())
                .parallelismHint(2)
                .shuffle()
                .each(new Fields("str"), new MapperFunction(), new Fields("flows"))
                .project(new Fields("flows"));

        Stream locationStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow", "rb_loc"), new Fields("mseMap"))
                .project(new Fields("flows", "mseMap"));

        Stream trapStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(rssiState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow", "rb_trap"), new Fields("rssiMap"))
                .project(new Fields("flows", "rssiMap"));

        Stream macVendorStream = flowStream
                .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"));

        Stream geoIPStream = flowStream
                .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"));

        Stream httpUrlStream = flowStream
                .each(new Fields("flows"), new AnalizeHttpUrlFunction(), new Fields("httpUrlMap"));

        Stream mobileStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("src"), new Fields("src_ip_addr"))
                .stateQuery(mobileState, new Fields("src_ip_addr"), new MemcachedQuery("src_ip_addr", "rb_mobile"), new Fields("ipAssignMap"))
                .each(new Fields("ipAssignMap"), new GetFieldFunction("imsi"), new Fields("imsi"))
                .stateQuery(mobileState, new Fields("imsi"), new MemcachedQuery("imsi", "rb_mobile"), new Fields("ueRegisterMap"))
                .each(new Fields("ueRegisterMap"), new GetFieldFunction("path"), new Fields("path"))
                .stateQuery(mobileState, new Fields("path"), new MemcachedQuery("path", "rb_mobile"), new Fields("hnbRegisterMap"))
                .each(new Fields("ipAssignMap", "ueRegisterMap", "hnbRegisterMap"), new JoinFlowFunction(), new Fields("mobileMap"))
                .project(new Fields("flows", "mobileMap"));

        List<Stream> joinStream = new ArrayList<>();
        joinStream.add(locationStream);
        joinStream.add(macVendorStream);
        joinStream.add(geoIPStream);
        joinStream.add(mobileStream);
        joinStream.add(trapStream);
        joinStream.add(httpUrlStream);

        List<Fields> keyFields = new ArrayList<>();
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));

        Stream joinedStream = topology.join(joinStream, keyFields, new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap", "rssiMap", "httpUrlMap"))
                .each(new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap", "rssiMap", "httpUrlMap"), new JoinFlowFunction(), new Fields("finalMap"));

        String outputTopic = kafkaConfig.getOutputTopic();
        if (outputTopic != null) {

            System.out.println("Flows send to: " + outputTopic);
            joinedStream.each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"))
                    .each(new Fields("jsonString"), new ProducerKafkaFunction(kafkaConfig, outputTopic), new Fields("a"))
                    .parallelismHint(2);

            mseStream
                    .each(new Fields("mse_data_druid"), new MapToJSONFunction(), new Fields("jsonString"))
                    .each(new Fields("jsonString"), new ProducerKafkaFunction(kafkaConfig, outputTopic), new Fields("a"));
        } else {

            System.out.println("Flows send to indexing service.");

            StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow());

            joinedStream//.each(new Fields("finalMap"), new PrinterFunction("----"), new Fields(""))
                    .partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                    .parallelismHint(2);

            mseStream
                    .project(new Fields("mse_data_druid"))
                    .partitionPersist(druidStateFlow, new Fields("mse_data_druid"), new TridentBeamStateUpdater());
        }

        return topology;
    }

}
