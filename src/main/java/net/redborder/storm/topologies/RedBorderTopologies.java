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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import net.redborder.storm.function.MapperFunction;
import net.redborder.storm.function.GeoIpFunction;
import net.redborder.storm.function.GetFieldFunction;
import net.redborder.storm.function.GetID;
import net.redborder.storm.function.GetMSEdata;
import net.redborder.storm.function.GetTweetID;
import net.redborder.storm.function.JoinFlowFunction;
import net.redborder.storm.function.MacVendorFunction;
import net.redborder.storm.function.MapToJSONFunction;
import net.redborder.storm.function.MobileBuilderFunction;
import net.redborder.storm.function.PrinterFunction;
import net.redborder.storm.function.ProducerKafkaFunction;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.spout.TwitterStreamTridentSpout;
import net.redborder.storm.state.MemcachedState;
import net.redborder.storm.state.query.MseQuery;
import net.redborder.storm.state.query.TwitterQuery;
import net.redborder.storm.state.updater.mseUpdater;
import net.redborder.storm.state.updater.twitterUpdater;
import net.redborder.storm.util.GetKafkaConfig;
import net.redborder.storm.util.GetMemcachedConfig;
import net.redborder.storm.util.RBEventType;
import net.redborder.storm.util.druid.MyBeamFactoryMapEvent;
import net.redborder.storm.util.druid.MyBeamFactoryMapFlow;
import net.redborder.storm.util.druid.MyBeamFactoryMapMonitor;
import net.redborder.storm.util.state.ConcatKeyBuilder;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.state.StateFactory;

/**
 *
 * @author andresgomez
 */
public class RedBorderTopologies {

    public TridentTopology twitterTopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();
        int PORT = 52030;

        MemcachedState.Options twitterOpts = new MemcachedState.Options();
        twitterOpts.expiration = 10000;
        StateFactory memcached = MemcachedState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), twitterOpts);
        TridentState tweetState = topology.newStream("twitterStream", new TwitterStreamTridentSpout())
                .each(new Fields("tweet"), new MapperFunction(), new Fields("tweetMap"))
                .project(new Fields("tweetMap"))
                .each(new Fields("tweetMap"), new GetTweetID(), new Fields("userTwitterID"))
                .partitionBy(new Fields("userTwitterID"))
                .partitionPersist(memcached, new Fields("tweetMap", "userTwitterID"), new twitterUpdater());

        zkConfig.setTopicInt(RBEventType.MONITOR);
        topology.newStream("rb_monitor", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .each(new Fields("event"), new GetID(), new Fields("id"))
                .stateQuery(tweetState, new Fields("id", "event"), new TwitterQuery(), new Fields("eventTwitter"))
                .project(new Fields("eventTwitter"))
                .each(new Fields("eventTwitter"), new PrinterFunction("----"), new Fields("a"));

        return topology;
    }

    public TridentTopology kafkaToDruidTopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();

        zkConfig.setTopicInt(RBEventType.MONITOR);
        StateFactory druidStateMonitor = new TridentBeamStateFactory<>(new MyBeamFactoryMapMonitor(zkConfig));

        topology.newStream("rb_monitor", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .partitionPersist(druidStateMonitor, new Fields("event"), new TridentBeamStateUpdater());

        zkConfig.setTopicInt(RBEventType.EVENT);
        StateFactory druidStateEvent = new TridentBeamStateFactory<>(new MyBeamFactoryMapEvent(zkConfig));

        topology.newStream("rb_event", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .partitionPersist(druidStateEvent, new Fields("event"), new TridentBeamStateUpdater());

        zkConfig.setTopicInt(RBEventType.FLOW);
        StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(zkConfig));

        topology.newStream("rb_flow", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .partitionPersist(druidStateFlow, new Fields("event"), new TridentBeamStateUpdater());

        return topology;
    }

    public TridentTopology flowMSEtopologyDelay() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();

        int PORT = 52030;

        MemcachedState.Options mseOpts = new MemcachedState.Options();
        mseOpts.expiration = 20000;
        StateFactory memcached = MemcachedState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), mseOpts);

        zkConfig.setTopicInt(RBEventType.MSE);
        TridentState mseState = topology.newStream("rb_mse", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("mseMap"))
                .project(new Fields("mseMap"))
                .each(new Fields("mseMap"), new GetMSEdata(), new Fields("mac_src_mse", "geoLocationMSE"))
                .project(new Fields("mac_src_mse", "geoLocationMSE"))
                .partitionBy(new Fields("mac_src_mse"))
                .partitionPersist(memcached, new Fields("geoLocationMSE", "mac_src_mse"), new mseUpdater("mac_src_mse", "geoLocationMSE"));

        zkConfig.setTopicInt(RBEventType.FLOW);
        // MemcachedMultipleState.Options flowOpts = new MemcachedMultipleState.Options();
        //flowOpts.expiration = 20000;
        //flowOpts.keyBuilder = new ConcatKeyBuilder("flowsNOenriched");
        //StateFactory flowsNoMemcached = MemcachedMultipleState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), flowOpts);

        Stream flowsMse = topology.newStream("rb_flow", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("flowsMap"))
                .each(new Fields("flowsMap"), new GetFieldFunction("mac_src"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow"), new MseQuery("mac_src_flow"), new Fields("flowMseOK", "flowMseKO"));

        zkConfig.setTopicInt(RBEventType.FLOW);
        StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(zkConfig));

        Properties props = new Properties();

        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        flowsMse
                .project(new Fields("flowMseKO"))
                .each(new Fields("flowMseKO"),  new FilterNull())
                .each(new Fields("flowMseKO"), new MapToJSONFunction(), new Fields("jsonFlowMseKO"))
                .project(new Fields("jsonFlowMseKO"))
                .each(new Fields("jsonFlowMseKO"), new ProducerKafkaFunction(props, "rb_delay"), new Fields("sendOK"));

        Stream tranquilityStream = topology.newStream("rb_delay", new TridentKafkaSpout().builder(
                "localhost:2181", "rb_delay", "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("flowMseKO"))
                .each(new Fields("flowMseKO"), new GetFieldFunction("mac_src"), new Fields("mac_src_flow_KO"))
                .stateQuery(mseState, new Fields("mac_src_flow_KO", "flowMseKO"), new MseQuery("mac_src_flow_KO"), new Fields("flowsTranquilityOK", "flowsTranquilityKO"));

        tranquilityStream
                .project(new Fields("flowsTranquilityOK"))
                .each(new Fields("flowsTranquilityOK"), new FilterNull())
                .each(new Fields("flowsTranquilityOK"), new PrinterFunction("----"), new Fields("a"));
        //.partitionPersist(druidStateFlow, new Fields("flowsTranquility"), new TridentBeamStateUpdater());

        tranquilityStream
                .project(new Fields("flowsTranquilityKO"))
                .each(new Fields("flowsTranquilityKO"), new FilterNull())
                .each(new Fields("flowsTranquilityKO"), new PrinterFunction("----"), new Fields("b"));
        //.partitionPersist(druidStateFlow, new Fields("flowsTranquility"), new TridentBeamStateUpdater());

        flowsMse
                .project(new Fields("flowMseOK"))
                .each(new Fields("flowMseOK"), new FilterNull())
                .each(new Fields("flowMseOK"), new PrinterFunction("----"), new Fields("a"));;

        //.partitionPersist(druidStateFlow, new Fields("flowMseOK"), new TridentBeamStateUpdater());
        return topology;
    }

    public TridentTopology flowMSEtopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        GetKafkaConfig zkConfig = new GetKafkaConfig();

        int PORT = 52030;

        MemcachedState.Options mseOpts = new MemcachedState.Options();
        mseOpts.expiration = 20000;
        StateFactory memcached = MemcachedState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), mseOpts);

        zkConfig.setTopicInt(RBEventType.MSE);
        TridentState mseState = topology.newStream("rb_mse", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("mseMap"))
                .project(new Fields("mseMap"))
                .each(new Fields("mseMap"), new GetMSEdata(), new Fields("mac_src_mse", "geoLocationMSE"))
                .project(new Fields("mac_src_mse", "geoLocationMSE"))
                .partitionBy(new Fields("mac_src_mse"))
                .partitionPersist(memcached, new Fields("geoLocationMSE", "mac_src_mse"), new mseUpdater("mac_src_mse", "geoLocationMSE"));

        zkConfig.setTopicInt(RBEventType.FLOW);

        Stream flowsMse = topology.newStream("rb_flow", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("flowsMap"))
                .each(new Fields("flowsMap"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow", "flowsMap"), new MseQuery("mac_src_flow"), new Fields("flowMSE"))
                .each(new Fields("flowMSE"), new PrinterFunction("----"), new Fields("a"));;

        return topology;
    }

    public TridentTopology Test() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();
        GetMemcachedConfig memConfig = new GetMemcachedConfig();
        MemcachedState.Options mseOpts = new MemcachedState.Options();
        
        memConfig.builder();
        mseOpts.expiration = 60000;
        
        StateFactory memcached = MemcachedState.transactional(memConfig.getConfig(), mseOpts);

        zkConfig.setTopicInt(RBEventType.MSE);

        TridentState mseState = topology.newStream("rb_mse", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("mse_map"))
                .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data"))
                .partitionPersist(memcached, new Fields("src_mac", "mse_data"), new mseUpdater("src_mac", "mse_data"))
                .parallelismHint(2);

        zkConfig.setTopicInt(RBEventType.FLOW);
        
        //StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(zkConfig));

        Stream flowStream = topology.newStream("rb_flow", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MapperFunction(), new Fields("flows"))
                .project(new Fields("flows"))
                .parallelismHint(2);

        Stream locationStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow"), new MseQuery("mac_src_flow"), new Fields("mseMap"))
                .project(new Fields("flows", "mseMap"))
                .parallelismHint(2);

        Stream macVendorStream = flowStream
                .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"))
                .parallelismHint(2);
        
        Stream geoIPStream = flowStream
                .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"))
                .parallelismHint(2);
        
        List<Stream> joinStream = new ArrayList<>();
        joinStream.add(locationStream);
        joinStream.add(macVendorStream);
        joinStream.add(geoIPStream);

        List<Fields> keyFields = new ArrayList<>();
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));

        topology.join(joinStream, keyFields, new Fields("flows", "mseMap", "macVendorMap", "geoIPMap"))
                .each(new Fields("flows", "mseMap", "macVendorMap", "geoIPMap"), new JoinFlowFunction(), new Fields("finalMap"))
                .each(new Fields("finalMap"), new PrinterFunction("----"), new Fields(""))
                //.partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                .parallelismHint(2);
        
        return topology;
    }
    
    public TridentTopology Mobile() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();

        zkConfig.setTopicInt(RBEventType.MOBILE);

        topology.newStream("rb_mobile", new TridentKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new MobileBuilderFunction(), new Fields("mobile"))
                .each(new Fields("mobile"), new PrinterFunction("----"), new Fields("a"));
                //.parallelismHint(5);

        return topology;
    }
}
