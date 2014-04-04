/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redborder.storm.trident.topologies;

import backtype.storm.tuple.Fields;
import com.metamx.tranquility.storm.TridentBeamStateFactory;
import com.metamx.tranquility.storm.TridentBeamStateUpdater;
import com.redborder.storm.trident.CorrelationTridentTopology;
import com.redborder.storm.trident.filter.MSEenrichedFilter;
import com.redborder.storm.trident.filter.SleepFilter;
import com.redborder.storm.trident.function.EventBuilderFunction;
import com.redborder.storm.trident.function.GetFieldFunction;
import com.redborder.storm.trident.spout.TrindetKafkaSpout;
import com.redborder.storm.trident.spout.TwitterStreamTridentSpout;
import com.redborder.storm.trident.state.MemcachedMultipleState;
import com.redborder.storm.trident.state.query.mseQuery;
import com.redborder.storm.trident.state.query.twitterQuery;
import com.redborder.storm.trident.updater.mseUpdater;
import com.redborder.storm.trident.updater.twitterUpdater;
import com.redborder.storm.util.GetKafkaConfig;
import com.redborder.storm.util.RBEventType;
import com.redborder.storm.util.druid.MyBeamFactoryMapEvent;
import com.redborder.storm.util.druid.MyBeamFactoryMapFlow;
import com.redborder.storm.util.druid.MyBeamFactoryMapMonitor;
import com.redborder.storm.util.state.ConcatKeyBuilder;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

/**
 *
 * @author andresgomez
 */
public class TridentRedBorderTopologies {

    public TridentTopology twitterTopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();
        int PORT = 52030;

        MemcachedMultipleState.Options twitterOpts = new MemcachedMultipleState.Options();
        twitterOpts.expiration = 10000;
        twitterOpts.keyBuilder = new ConcatKeyBuilder("Twitter");
        StateFactory memcached = MemcachedMultipleState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), twitterOpts);
        TridentState tweetState = topology.newStream("twitterStream", new TwitterStreamTridentSpout())
                .each(new Fields("tweet"), new EventBuilderFunction(5), new Fields("topic", "tweetMap"))
                .project(new Fields("tweetMap"))
                .each(new Fields("tweetMap"), new CorrelationTridentTopology.GetTweetID(), new Fields("userTwitterID"))
                .partitionBy(new Fields("userTwitterID"))
                .partitionPersist(memcached, new Fields("tweetMap", "userTwitterID"), new twitterUpdater());

        zkConfig.setTopicInt(RBEventType.MONITOR);
        topology.newStream("rb_monitor", new TrindetKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new EventBuilderFunction(RBEventType.MONITOR), new Fields("topic", "event"))
                .each(new Fields("event"), new CorrelationTridentTopology.GetID(), new Fields("id"))
                .stateQuery(tweetState, new Fields("id", "event"), new twitterQuery(), new Fields("eventTwitter"))
                .project(new Fields("eventTwitter"))
                .each(new Fields("eventTwitter"), new CorrelationTridentTopology.PrinterBolt("----"), new Fields("a"));

        return topology;
    }

    public TridentTopology kafkaToDruidTopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();

        zkConfig.setTopicInt(RBEventType.MONITOR);
        StateFactory druidStateMonitor = new TridentBeamStateFactory<>(new MyBeamFactoryMapMonitor(zkConfig));

        topology.newStream("rb_monitor", new TrindetKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new EventBuilderFunction(RBEventType.MONITOR), new Fields("topic", "event"))
                .partitionPersist(druidStateMonitor, new Fields("event"), new TridentBeamStateUpdater());

        zkConfig.setTopicInt(RBEventType.EVENT);
        StateFactory druidStateEvent = new TridentBeamStateFactory<>(new MyBeamFactoryMapEvent(zkConfig));

        topology.newStream("rb_event", new TrindetKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new EventBuilderFunction(RBEventType.EVENT), new Fields("topic", "event"))
                .partitionPersist(druidStateEvent, new Fields("event"), new TridentBeamStateUpdater());

        zkConfig.setTopicInt(RBEventType.FLOW);
        StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(zkConfig));

        topology.newStream("rb_flow", new TrindetKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new EventBuilderFunction(RBEventType.FLOW), new Fields("topic", "event"))
                .partitionPersist(druidStateFlow, new Fields("event"), new TridentBeamStateUpdater());

        return topology;
    }

    public TridentTopology flowMSEtopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        GetKafkaConfig zkConfig = new GetKafkaConfig();

        int PORT = 52030;

        MemcachedMultipleState.Options mseOpts = new MemcachedMultipleState.Options();
        mseOpts.expiration = 20000;
        mseOpts.keyBuilder = new ConcatKeyBuilder("MSE");
        StateFactory memcached = MemcachedMultipleState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), mseOpts);

        zkConfig.setTopicInt(RBEventType.MSE);
        TridentState mseState = topology.newStream("rb_mse", new TrindetKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new EventBuilderFunction(RBEventType.MSE), new Fields("topic", "mseMap"))
                .project(new Fields("mseMap"))
                .each(new Fields("mseMap"), new GetFieldFunction("mac_src"), new Fields("mac_src_mse"))
                .partitionBy(new Fields("mac_src"))
                .partitionPersist(memcached, new Fields("mseMap", "mac_src"), new mseUpdater());

        zkConfig.setTopicInt(RBEventType.FLOW);
        MemcachedMultipleState.Options flowOpts = new MemcachedMultipleState.Options();
        flowOpts.expiration = 20000;
        flowOpts.keyBuilder = new ConcatKeyBuilder("flowsNOenriched");
        StateFactory flowsNoMemcached = MemcachedMultipleState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), flowOpts);

        Stream flowsMse = topology.newStream("rb_flow", new TrindetKafkaSpout().builder(
                zkConfig.getZkConnect(), zkConfig.getTopic(), "kafkaStorm"))
                .each(new Fields("str"), new EventBuilderFunction(RBEventType.FLOW), new Fields("topic", "flowsMap"))
                .each(new Fields("flowsMap"), new GetFieldFunction("mac_src"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow", "flowsMap"), new mseQuery(), new Fields("flowMseOK", "flowMseKO"))
                .each(new Fields("flowMseOK"), new MSEenrichedFilter())
                .each(new Fields("flowMseKO"), new MSEenrichedFilter());

        zkConfig.setTopicInt(RBEventType.FLOW);
        StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(zkConfig));

        flowsMse
                .project(new Fields("flowMseKO"))
                .each(new Fields("flowMseKO"), new SleepFilter(10000l))
                .each(new Fields("flowMseKO"), new GetFieldFunction("mac_src"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow", "flowMseKO"), new mseQuery(), new Fields("flowsTranquility"))
                .partitionPersist(druidStateFlow, new Fields("flowsTranquility"), new TridentBeamStateUpdater());

        flowsMse
                .partitionPersist(druidStateFlow, new Fields("flowMseOK"), new TridentBeamStateUpdater());

        return topology;
    }

}
