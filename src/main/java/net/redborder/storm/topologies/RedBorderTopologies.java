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
import java.util.Properties;
import net.redborder.storm.function.AnalizeHttpUrlFunction;
import net.redborder.storm.function.GeoIpFunction;
import net.redborder.storm.function.GetFieldFunction;
import net.redborder.storm.function.GetID;
import net.redborder.storm.function.GetMSEdata;
import net.redborder.storm.function.GetRSSIdata;
import net.redborder.storm.function.GetTweetID;
import net.redborder.storm.function.JoinFlowFunction;
import net.redborder.storm.function.MacVendorFunction;
import net.redborder.storm.function.MapToJSONFunction;
import net.redborder.storm.function.MapperFunction;
import net.redborder.storm.function.MobileBuilderFunction;
import net.redborder.storm.function.PrinterFunction;
import net.redborder.storm.function.ProducerKafkaFunction;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.spout.TwitterStreamTridentSpout;
import net.redborder.storm.state.MemcachedState;
import net.redborder.storm.state.query.MemcachedQuery;
import net.redborder.storm.state.query.TwitterQuery;
import net.redborder.storm.state.updater.MemcachedUpdater;
import net.redborder.storm.state.updater.twitterUpdater;
import net.redborder.storm.util.MemcachedConfigFile;
import net.redborder.storm.util.druid.MyBeamFactoryMapEvent;
import net.redborder.storm.util.druid.MyBeamFactoryMapFlow;
import net.redborder.storm.util.druid.MyBeamFactoryMapMonitor;
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

    MemcachedConfigFile _memConfig;
    MemcachedState.Options _mseOpts;

    public RedBorderTopologies() throws FileNotFoundException {
        _memConfig = new MemcachedConfigFile();
        
        _mseOpts = new MemcachedState.Options();
        _mseOpts.expiration = 60000;
    }

    public TridentTopology twitterTopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        StateFactory memcached = MemcachedState.transactional(_memConfig.getConfig(), _mseOpts);

        TridentState tweetState = topology.newStream("twitterStream", new TwitterStreamTridentSpout())
                .each(new Fields("tweet"), new MapperFunction(), new Fields("tweetMap"))
                .project(new Fields("tweetMap"))
                .each(new Fields("tweetMap"), new GetTweetID(), new Fields("userTwitterID"))
                .partitionBy(new Fields("userTwitterID"))
                .partitionPersist(memcached, new Fields("tweetMap", "userTwitterID"), new twitterUpdater());

        topology.newStream("rb_monitor", new TridentKafkaSpout("monitor").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .each(new Fields("event"), new GetID(), new Fields("id"))
                .stateQuery(tweetState, new Fields("id", "event"), new TwitterQuery(), new Fields("eventTwitter"))
                .project(new Fields("eventTwitter"))
                .each(new Fields("eventTwitter"), new PrinterFunction("----"), new Fields("a"));

        return topology;
    }

    public TridentTopology kafkaToDruidTopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        StateFactory druidStateMonitor = new TridentBeamStateFactory<>(new MyBeamFactoryMapMonitor());

        topology.newStream("rb_monitor", new TridentKafkaSpout("mobile").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .partitionPersist(druidStateMonitor, new Fields("event"), new TridentBeamStateUpdater());

        StateFactory druidStateEvent = new TridentBeamStateFactory<>(new MyBeamFactoryMapEvent());

        topology.newStream("rb_event", new TridentKafkaSpout("events").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .partitionPersist(druidStateEvent, new Fields("event"), new TridentBeamStateUpdater());

        StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow());

        topology.newStream("rb_flow", new TridentKafkaSpout("traffics").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("event"))
                .partitionPersist(druidStateFlow, new Fields("event"), new TridentBeamStateUpdater());

        return topology;
    }

    public TridentTopology flowMSEtopologyDelay() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        StateFactory memcached = MemcachedState.transactional(_memConfig.getConfig(), _mseOpts);

        TridentState mseState = topology.newStream("rb_mse", new TridentKafkaSpout("location").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("mseMap"))
                .each(new Fields("mseMap"), new GetMSEdata(), new Fields("mac_src_mse", "geoLocationMSE"))
                .partitionPersist(memcached, new Fields("geoLocationMSE", "mac_src_mse"), new MemcachedUpdater("mac_src_mse", "geoLocationMSE"));

        // MemcachedMultipleState.Options flowOpts = new MemcachedMultipleState.Options();
        // flowOpts.expiration = 20000;
        // flowOpts.keyBuilder = new ConcatKeyBuilder("flowsNOenriched");
        // StateFactory flowsNoMemcached = MemcachedMultipleState.transactional(Arrays.asList(new InetSocketAddress("localhost", PORT)), flowOpts);
        Stream flowsMse = topology.newStream("rb_flow", new TridentKafkaSpout("traffics").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("flowsMap"))
                .each(new Fields("flowsMap"), new GetFieldFunction("mac_src"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow"), new Fields("flowMseOK", "flowMseKO"));

        StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow());

        Properties props = new Properties();

        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        flowsMse.project(new Fields("flowMseKO"))
                .each(new Fields("flowMseKO"), new FilterNull())
                .each(new Fields("flowMseKO"), new MapToJSONFunction(), new Fields("jsonFlowMseKO"))
                .project(new Fields("jsonFlowMseKO"))
                .each(new Fields("jsonFlowMseKO"), new ProducerKafkaFunction(props, "rb_delay"), new Fields("sendOK"));

        Stream tranquilityStream = topology.newStream("rb_delay", new TridentKafkaSpout("delay").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("flowMseKO"))
                .each(new Fields("flowMseKO"), new GetFieldFunction("mac_src"), new Fields("mac_src_flow_KO"))
                .stateQuery(mseState, new Fields("mac_src_flow_KO", "flowMseKO"), new MemcachedQuery("mac_src_flow_KO"), new Fields("flowsTranquilityOK", "flowsTranquilityKO"));

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
                .each(new Fields("flowMseOK"), new PrinterFunction("----"), new Fields("a"));
        //.partitionPersist(druidStateFlow, new Fields("flowMseOK"), new TridentBeamStateUpdater());

        return topology;
    }

    public TridentTopology flowMSEtopology() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        StateFactory memcached = MemcachedState.transactional(_memConfig.getConfig(), _mseOpts);

        TridentState mseState = topology.newStream("rb_mse", new TridentKafkaSpout("location").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("mseMap"))
                .each(new Fields("mseMap"), new GetMSEdata(), new Fields("mac_src_mse", "geoLocationMSE"))
                .partitionPersist(memcached, new Fields("geoLocationMSE", "mac_src_mse"), new MemcachedUpdater("mac_src_mse", "geoLocationMSE"));

        Stream flowsMse = topology.newStream("rb_flow", new TridentKafkaSpout("traffics").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("flowsMap"))
                .each(new Fields("flowsMap"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow", "flowsMap"), new MemcachedQuery("mac_src_flow"), new Fields("flowMSE"))
                .each(new Fields("flowMSE"), new PrinterFunction("----"), new Fields("a"));

        return topology;
    }

    public TridentTopology Test() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();
        StateFactory memcached = MemcachedState.transactional(_memConfig.getConfig(), _mseOpts);

        TridentState mseState = topology.newStream("rb_mse", new TridentKafkaSpout("location").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("mse_map"))
                .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data"))
                .partitionPersist(memcached, new Fields("src_mac", "mse_data"), new MemcachedUpdater("src_mac", "mse_data"))
                .parallelismHint(2);

        TridentState mobileState = topology.newStream("rb_mobile", new TridentKafkaSpout("mobile").builder())
                .each(new Fields("str"), new MobileBuilderFunction(), new Fields("ip_addr", "mobile"))
                .partitionPersist(memcached, new Fields("ip_addr", "mobile"), new MemcachedUpdater("ip_addr", "mobile"))
                .parallelismHint(2);

        //StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow(zkConfig));
        Stream flowStream = topology.newStream("rb_flow", new TridentKafkaSpout("traffics").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("flows"))
                .project(new Fields("flows"))
                .parallelismHint(2);

        Stream locationStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow"), new Fields("mseMap"))
                .project(new Fields("flows", "mseMap"))
                .parallelismHint(2);

        Stream macVendorStream = flowStream
                .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"))
                .parallelismHint(2);

        Stream geoIPStream = flowStream
                .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"))
                .parallelismHint(2);

        Stream imsiStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("src"), new Fields("src_ip_addr"))
                .stateQuery(mobileState, new Fields("src_ip_addr"), new MemcachedQuery("src_ip_addr"), new Fields("mobileMap"))
                .project(new Fields("flows", "mobileMap"))
                .parallelismHint(2);

        List<Stream> joinStream = new ArrayList<>();
        joinStream.add(locationStream);
        joinStream.add(macVendorStream);
        joinStream.add(geoIPStream);
        joinStream.add(imsiStream);

        List<Fields> keyFields = new ArrayList<>();
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));

        topology.join(joinStream, keyFields, new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap"))
                .each(new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap"), new JoinFlowFunction(), new Fields("finalMap"))
                .each(new Fields("finalMap"), new PrinterFunction("----"), new Fields(""))
                //.partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                .parallelismHint(2);

        return topology;
    }

    public TridentTopology Mobile() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        topology.newStream("rb_mobile", new TridentKafkaSpout("mobile").builder())
                .each(new Fields("str"), new MobileBuilderFunction(), new Fields("mobile"))
                //.each(new Fields("mobile"), new PrinterFunction("----"), new Fields("a"));
                .parallelismHint(2);

        return topology;
    }

    public TridentTopology Rssi() throws FileNotFoundException {

        TridentTopology topology = new TridentTopology();

        StateFactory memcached = MemcachedState.transactional(_memConfig.getConfig(), _mseOpts);

        TridentState rssiState = topology.newStream("rb_rssi", new TridentKafkaSpout("snmp").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("rssi"))
                .each(new Fields("rssi"), new GetRSSIdata(), new Fields("rssiKey", "rssiValue"))
                .partitionPersist(memcached, new Fields("rssiKey", "rssiValue"), new MemcachedUpdater("rssiKey", "rssiValue"));

        Stream flowStream = topology.newStream("rb_flow", new TridentKafkaSpout("traffics").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("flows"))
                .project(new Fields("flows"));

        Stream locationStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(rssiState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow"), new Fields("rssiMap"))
                .each(new Fields("rssiMap"), new PrinterFunction("----"), new Fields("a"));

        return topology;
    }

    public TridentTopology all() throws FileNotFoundException {
        TridentTopology topology = new TridentTopology();

        MemcachedState.Options mseOpts = new MemcachedState.Options();
        mseOpts.expiration = 60000;
        mseOpts.globalKey = "location";
        StateFactory memcachedLocation = MemcachedState.transactional(_memConfig.getConfig(), mseOpts);

        MemcachedState.Options rssiOpts = new MemcachedState.Options();
        mseOpts.expiration = 60000;
        mseOpts.globalKey = "rssi";
        StateFactory memcachedRssi = MemcachedState.transactional(_memConfig.getConfig(), mseOpts);

        MemcachedState.Options mobileOpts = new MemcachedState.Options();
        mseOpts.expiration = 60000;
        mseOpts.globalKey = "mobile";
        StateFactory memcachedMobile = MemcachedState.transactional(_memConfig.getConfig(), mseOpts);

        /* LOCATION DATA */
        TridentState mseState = topology.newStream("rb_mse", new TridentKafkaSpout("location").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("mse_map"))
                .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data"))
                .partitionPersist(memcachedLocation, new Fields("src_mac", "mse_data"), new MemcachedUpdater("src_mac", "mse_data"))
                .parallelismHint(2);

        /* MOBILE DATA */
        TridentState mobileState = topology.newStream("rb_mobile", new TridentKafkaSpout("mobile").builder())
                .each(new Fields("str"), new MobileBuilderFunction(), new Fields("ip_addr", "mobile"))
                .partitionPersist(memcachedMobile, new Fields("ip_addr", "mobile"), new MemcachedUpdater("ip_addr", "mobile"))
                .parallelismHint(2);

        /* RSSI DATA */
        TridentState rssiState = topology.newStream("rb_rssi", new TridentKafkaSpout("snmp").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("rssi"))
                .each(new Fields("rssi"), new GetRSSIdata(), new Fields("rssiKey", "rssiValue"))
                .partitionPersist(memcachedRssi, new Fields("rssiKey", "rssiValue"), new MemcachedUpdater("rssiKey", "rssiValue"))
                .parallelismHint(2);

        /* FLOW STREAM */
        Stream flowStream = topology.newStream("rb_flow", new TridentKafkaSpout("traffics").builder())
                .each(new Fields("str"), new MapperFunction(), new Fields("flows"))
                .project(new Fields("flows"))
                .parallelismHint(2);

        Stream locationStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(mseState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow"), new Fields("mseMap"))
                .project(new Fields("flows", "mseMap"))
                .parallelismHint(2);

        Stream rssiStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("client_mac"), new Fields("mac_src_flow"))
                .stateQuery(rssiState, new Fields("mac_src_flow"), new MemcachedQuery("mac_src_flow"), new Fields("rssiMap"))
                .project(new Fields("flows", "rssiMap"))
                .parallelismHint(2);

        Stream macVendorStream = flowStream
                .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendorMap"))
                .parallelismHint(2);

        Stream geoIPStream = flowStream
                .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"))
                .parallelismHint(2);

        Stream httpUrlStream = flowStream
                .each(new Fields("flows"), new AnalizeHttpUrlFunction(), new Fields("httpUrlMap"))
                .parallelismHint(2);

        Stream imsiStream = flowStream
                .each(new Fields("flows"), new GetFieldFunction("src"), new Fields("src_ip_addr"))
                .stateQuery(mobileState, new Fields("src_ip_addr"), new MemcachedQuery("src_ip_addr"), new Fields("mobileMap"))
                .project(new Fields("flows", "mobileMap"))
                .parallelismHint(2);

        List<Stream> joinStream = new ArrayList<>();
        joinStream.add(locationStream);
        joinStream.add(macVendorStream);
        joinStream.add(geoIPStream);
        joinStream.add(imsiStream);
        joinStream.add(rssiStream);
        joinStream.add(httpUrlStream);

        List<Fields> keyFields = new ArrayList<>();
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));
        keyFields.add(new Fields("flows"));

        StateFactory druidStateFlow = new TridentBeamStateFactory<>(new MyBeamFactoryMapFlow());
        
        topology.join(joinStream, keyFields, new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap", "rssiMap", "httpUrlMap"))
                .each(new Fields("flows", "mseMap", "macVendorMap", "geoIPMap", "mobileMap", "rssiMap", "httpUrlMap"), new JoinFlowFunction(), new Fields("finalMap"))
                //.each(new Fields("finalMap"), new PrinterFunction("----"), new Fields(""))
                .partitionPersist(druidStateFlow, new Fields("finalMap"), new TridentBeamStateUpdater())
                .parallelismHint(6);

        return topology;
    }
}
