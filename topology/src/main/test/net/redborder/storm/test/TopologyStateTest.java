package net.redborder.storm.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import net.redborder.state.gridgain.GridGainFactory;
import net.redborder.storm.function.*;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.state.LocationQuery;
import net.redborder.storm.state.StateQuery;
import org.codehaus.jackson.map.ObjectMapper;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheMode;
import org.junit.Assert;
import org.junit.Test;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by andresgomez on 14/07/14.
 */
public class TopologyStateTest {

    @Test
    public void locationDataTest() throws GridException, IOException {
        List<String> topics = new ArrayList<String>();
        topics.add("location");

        List<String> fieldsFlow = new ArrayList<String>();

        fieldsFlow.add("flows");
        fieldsFlow.add("mseMap");

        GridConfiguration ggConf = new GridConfiguration();
        GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
        cacheLocation.setName("location");
        cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);
        ggConf.setCacheConfiguration(cacheLocation);
        Grid grid = GridGain.start(ggConf);

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();

        GridGainFactory locationStateFactory = new GridGainFactory("location", topics);
        TridentState locationState = topology.newStaticState(locationStateFactory);

        topology.newDRPCStream("location", drpc)
                .each(new Fields("args"), new MapperFunction("rb_loc"), new Fields("mse_map"))
                .each(new Fields("mse_map"), new GetMSEdata(), new Fields("src_mac", "mse_data", "mse_data_druid"))
                .project(new Fields("src_mac", "mse_data"))
                .name("locationStream");

        topology.newDRPCStream("flow", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .stateQuery(locationState, new Fields("flows"),
                        new LocationQuery("client_mac"), new Fields("mseMap"))
                .each(new Fields(fieldsFlow), new MergeMapsFunction(), new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"))
                .project(new Fields("jsonString"));


        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        File locationFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/locationData.json").getPath());

        Scanner locationData = new Scanner(locationFile);

        while (locationData.hasNextLine()) {

            String mse = drpc.execute("location", locationData.nextLine());
            mse = mse.substring(mse.indexOf("[[") + 2, mse.indexOf("]]"));

            GridCache<String, Map<String, Object>> cache = grid.cache("location");

            String[] mseSplit = mse.split(",", 2);

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(mseSplit[1], Map.class);

            String key = mseSplit[0].substring(1, mseSplit[0].length() - 1);

            cache.put(key, map);
        }

        File flowFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/flowsNetFlow10.json").getPath());
        File checkFile = new File(Thread.currentThread().getContextClassLoader().getResource("dataCheck/mseData.json").getPath());

        Scanner flowData = new Scanner(flowFile);
        Scanner checkData = new Scanner(checkFile);


        while (flowData.hasNextLine()) {
            String stormFlow = drpc.execute("flow", flowData.nextLine());
            stormFlow=stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            Assert.assertEquals(checkData.nextLine(), stormFlow);
        }

        grid.close();
    }

    @Test
    public void radiusDataTest() throws GridException, IOException {
        List<String> topics = new ArrayList<String>();
        topics.add("radius");

        List<String> fieldsFlow = new ArrayList<String>();

        fieldsFlow.add("flows");
        fieldsFlow.add("radiusMap");

        GridConfiguration ggConf = new GridConfiguration();
        GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
        cacheLocation.setName("radius");
        cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);
        ggConf.setCacheConfiguration(cacheLocation);
        Grid grid = GridGain.start(ggConf);

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();

        GridGainFactory radiusStateFactory = new GridGainFactory("radius", topics);
        TridentState radiusState = topology.newStaticState(radiusStateFactory);

        topology.newDRPCStream("radius", drpc)
                .each(new Fields("args"), new MapperFunction("rb_radius"), new Fields("radius"))
                .each(new Fields("radius"), new GetRadiusClient(), new Fields("clientMap"))
                .stateQuery(radiusState, new Fields("clientMap"), new StateQuery("client_mac"),
                        new Fields("radiusCached"))
                .each(new Fields("radius", "radiusCached"), new GetRadiusData(),
                        new Fields("radiusKey", "radiusData", "radiusDruid"))
                .project(new Fields("radiusKey", "radiusData"));

        topology.newDRPCStream("flow", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .stateQuery(radiusState, new Fields("flows"),
                        new StateQuery("client_mac"), new Fields("radiusMap"))
                .each(new Fields(fieldsFlow), new MergeMapsFunction(), new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"))
                .project(new Fields("jsonString"));


        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        File locationFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/radiusData.json").getPath());

        Scanner locationData = new Scanner(locationFile);

        while (locationData.hasNextLine()) {

            String radius = drpc.execute("radius", locationData.nextLine());
            radius = radius.substring(radius.indexOf("[[") + 2, radius.indexOf("]]"));

            GridCache<String, Map<String, Object>> cache = grid.cache("radius");

            String[] radiusSplit = radius.split(",", 2);

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(radiusSplit[1], Map.class);

            String key = radiusSplit[0].substring(1, radiusSplit[0].length() - 1);

            cache.put(key, map);
        }

        File flowFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/flowToRadius.json").getPath());
        File checkFile = new File(Thread.currentThread().getContextClassLoader().getResource("dataCheck/radiusData.json").getPath());

        Scanner flowData = new Scanner(flowFile);
        Scanner checkData = new Scanner(checkFile);


        while (flowData.hasNextLine()) {
            String stormFlow = drpc.execute("flow", flowData.nextLine());
            stormFlow=stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            Assert.assertEquals(checkData.nextLine(), stormFlow);
        }

        grid.close();
    }

    @Test
    public void trapDataTest() throws GridException, IOException {
        List<String> topics = new ArrayList<String>();
        topics.add("trap");

        List<String> fieldsFlow = new ArrayList<String>();

        fieldsFlow.add("flows");
        fieldsFlow.add("trapMap");

        GridConfiguration ggConf = new GridConfiguration();
        GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
        cacheLocation.setName("trap");
        cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);
        ggConf.setCacheConfiguration(cacheLocation);
        Grid grid = GridGain.start(ggConf);

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();

        GridGainFactory trapStateFactory = new GridGainFactory("trap", topics);
        TridentState trapState = topology.newStaticState(trapStateFactory);

        topology.newDRPCStream("trap", drpc)
                .each(new Fields("args"), new MapperFunction("rb_trap"), new Fields("rssi"))
                .each(new Fields("rssi"), new GetTRAPdata(), new Fields("rssiKey", "rssiValue"))
                .project(new Fields("rssiKey", "rssiValue"));

        topology.newDRPCStream("flow", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .stateQuery(trapState, new Fields("flows"),
                        new StateQuery("client_mac"), new Fields("trapMap"))
                .each(new Fields(fieldsFlow), new MergeMapsFunction(), new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"))
                .project(new Fields("jsonString"));


        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        File locationFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/trapData.json").getPath());

        Scanner locationData = new Scanner(locationFile);

        while (locationData.hasNextLine()) {

            String trap = drpc.execute("trap", locationData.nextLine());
            trap = trap.substring(trap.indexOf("[[") + 2, trap.indexOf("]]"));

            GridCache<String, Map<String, Object>> cache = grid.cache("trap");

            String[] trapSplit = trap.split(",", 2);

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(trapSplit[1], Map.class);

            String key = trapSplit[0].substring(1, trapSplit[0].length() - 1);

            cache.put(key, map);
        }

        File flowFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/flowToTrap.json").getPath());
        File checkFile = new File(Thread.currentThread().getContextClassLoader().getResource("dataCheck/trapData.json").getPath());

        Scanner flowData = new Scanner(flowFile);
        Scanner checkData = new Scanner(checkFile);


        while (flowData.hasNextLine()) {
            String stormFlow = drpc.execute("flow", flowData.nextLine());
            stormFlow=stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            Assert.assertEquals(checkData.nextLine(), stormFlow);
        }
        grid.close();
    }

    @Test
    public void mobileDataTest() throws GridException, IOException {
        List<String> topics = new ArrayList<String>();
        topics.add("mobile");

        List<String> fieldsFlow = new ArrayList<String>();

        fieldsFlow.add("flows");
        fieldsFlow.add("ipAssignMap");
        fieldsFlow.add("ueRegisterMap");
        fieldsFlow.add("hnbRegisterMap");

        GridConfiguration ggConf = new GridConfiguration();
        GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
        cacheLocation.setName("mobile");
        cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);
        ggConf.setCacheConfiguration(cacheLocation);
        Grid grid = GridGain.start(ggConf);

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();

        GridGainFactory mobileStateFactory = new GridGainFactory("mobile", topics);
        TridentState mobileState = topology.newStaticState(mobileStateFactory);

        topology.newDRPCStream("mobile", drpc)
                .each(new Fields("args"), new MobileBuilderFunction(), new Fields("key", "mobileMap"))
                .project(new Fields("key", "mobileMap"));

        topology.newDRPCStream("flow", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .stateQuery(mobileState, new Fields("flows"), new StateQuery("src"), new Fields("ipAssignMap"))
                .stateQuery(mobileState, new Fields("ipAssignMap"), new StateQuery("client_id"), new Fields("ueRegisterMap"))
                .stateQuery(mobileState, new Fields("ueRegisterMap"), new StateQuery("path"), new Fields("hnbRegisterMap"))
                .each(new Fields(fieldsFlow), new MergeMapsFunction(), new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"))
                .project(new Fields("jsonString"));


        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        File locationFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/mobileData.json").getPath());

        Scanner mobileData = new Scanner(locationFile);

        while (mobileData.hasNextLine()) {

            String mobile = drpc.execute("mobile", mobileData.nextLine());
            mobile = mobile.substring(mobile.indexOf("[[") + 2, mobile.indexOf("]]"));

            GridCache<String, Map<String, Object>> cache = grid.cache("mobile");

            String[] mobileSplit = mobile.split(",", 2);

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(mobileSplit[1], Map.class);

            String key = mobileSplit[0].substring(1, mobileSplit[0].length() - 1);

            cache.put(key, map);
        }

        File flowFile = new File(Thread.currentThread().getContextClassLoader().getResource("inputData/flowToMobile.json").getPath());
        File checkFile = new File(Thread.currentThread().getContextClassLoader().getResource("dataCheck/mobileData.json").getPath());

        Scanner flowData = new Scanner(flowFile);
        Scanner checkData = new Scanner(checkFile);


        while (flowData.hasNextLine()) {
            String stormFlow = drpc.execute("flow", flowData.nextLine());
            stormFlow=stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            Assert.assertEquals(checkData.nextLine(), stormFlow);
        }
        grid.close();
    }
}
