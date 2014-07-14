package net.redborder.storm.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import net.redborder.state.gridgain.GridGainFactory;
import net.redborder.storm.function.GetMSEdata;
import net.redborder.storm.function.MapToJSONFunction;
import net.redborder.storm.function.MapperFunction;
import net.redborder.storm.function.MergeMapsFunction;
import net.redborder.storm.state.LocationQuery;
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
    public void mseDataTest() throws GridException, IOException {
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

        File locationFile = new File("topology/src/main/resources/inputData/locationData.json");

        Scanner locationData = new Scanner(locationFile);

        while(locationData.hasNextLine()) {

            String mse = drpc.execute("location", locationData.nextLine());
            mse = mse.substring(mse.indexOf("[[") + 2, mse.indexOf("]]"));

            GridCache<String, Map<String, Object>> cache = grid.cache("location");

            String[] mseSplit = mse.split(",", 2);

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = mapper.readValue(mseSplit[1], Map.class);

            String key = mseSplit[0].substring(1, mseSplit[0].length() - 1);

            cache.put(key, map);
        }

        File flowFile = new File("topology/src/main/resources/inputData/flowsNetFlow10.json");
        File checkFile = new File("topology/src/main/resources/dataCheck/mseData.json");

        Scanner flowData = new Scanner(flowFile);
        Scanner checkData = new Scanner(checkFile);


        while(flowData.hasNextLine()) {
            String stormFlow = drpc.execute("flow", flowData.nextLine());
            Assert.assertEquals(checkData.nextLine(), stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1));
        }
    }
}
