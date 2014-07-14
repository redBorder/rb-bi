package net.redborder.storm.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import junit.framework.TestCase;
import net.redborder.state.gridgain.GridGainFactory;
import net.redborder.storm.function.*;
import net.redborder.storm.function.AnalizeHttpUrlFunction;
import net.redborder.storm.function.CheckTimestampFunction;
import net.redborder.storm.function.GeoIpFunction;
import net.redborder.storm.function.MacVendorFunction;
import net.redborder.storm.function.MapToJSONFunction;
import net.redborder.storm.function.MapperFunction;
import net.redborder.storm.spout.TridentKafkaSpout;
import net.redborder.storm.state.LocationQuery;
import net.redborder.storm.state.StateUpdater;
import net.redborder.storm.test.functions.FileFunction;
import net.redborder.storm.test.spout.SpoutFile;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by andresgomez on 14/07/14.
 */


public class TopologyFunctionTest {

    @Test
    public void geoIpTest() throws FileNotFoundException {

        File fileFlow = new File("topology/src/main/resources/inputData/flows.json");
        File checkFlow = new File("topology/src/main/resources/dataCheck/geoIpFlows.json");

        Scanner flows = new Scanner(fileFlow);
        Scanner checkFlows = new Scanner(checkFlow);

        GeoIpFunction.CITY_DB_PATH = "topology/src/main/resources/db/city.dat";
        GeoIpFunction.CITY_V6_DB_PATH = "topology/src/main/resources/db/cityv6.dat";
        GeoIpFunction.ASN_DB_PATH = "topology/src/main/resources/db/asn.dat";
        GeoIpFunction.ASN_V6_DB_PATH = "topology/src/main/resources/db/asnv6.dat";

        List<String> fieldsFlow = new ArrayList<String>();

        fieldsFlow.add("flows");
        fieldsFlow.add("geoIPMap");

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("test", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .each(new Fields("flows"), new GeoIpFunction(), new Fields("geoIPMap"))
                .each(new Fields(fieldsFlow), new MergeMapsFunction(), new Fields("finalMap"))
                .project(new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"));

        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        while (flows.hasNextLine()) {
            String stormFlow = drpc.execute("test", flows.nextLine());
            stormFlow = stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            //System.out.println(stormFlow);
            Assert.assertEquals(checkFlows.nextLine(), stormFlow);
        }
    }

    @Test
    public void macVendorTest() throws FileNotFoundException {

        File fileFlow = new File("topology/src/main/resources/inputData/flows.json");
        File checkFlow = new File("topology/src/main/resources/dataCheck/macVendorFlows.json");

        Scanner flows = new Scanner(fileFlow);
        Scanner checkFlows = new Scanner(checkFlow);

        MacVendorFunction._ouiFilePath = "topology/src/main/resources/db/oui-vendors";

        List<String> fieldsFlow = new ArrayList<String>();

        fieldsFlow.add("flows");
        fieldsFlow.add("macVendor");

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("test", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .each(new Fields("flows"), new MacVendorFunction(), new Fields("macVendor"))
                .each(new Fields(fieldsFlow), new MergeMapsFunction(), new Fields("finalMap"))
                .project(new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"));

        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        while (flows.hasNextLine()) {
            String stormFlow = drpc.execute("test", flows.nextLine());
            stormFlow = stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            Assert.assertEquals(checkFlows.nextLine(), stormFlow);
        }
    }


    @Test
    public void nonTimestampTest() throws FileNotFoundException {

        File fileFlow = new File("topology/src/main/resources/inputData/nonTimestampFlows.json");

        Scanner flows = new Scanner(fileFlow);


        List<String> fieldsFlow = new ArrayList<String>();

        fieldsFlow.add("flows");
        fieldsFlow.add("httpMap");

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("test", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .each(new Fields("flows"), new CheckTimestampFunction(), new Fields("finalMap"))
                .project(new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"));

        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        while (flows.hasNextLine()) {
            String stormFlow = drpc.execute("test", flows.nextLine());
            stormFlow = stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            Assert.assertEquals(true, stormFlow.contains("timestamp"));
        }
    }



    /*
    @Test
    public void separateLongTimeFlowTest() throws FileNotFoundException {

        DateTime minute = new DateTime().withMinuteOfHour(0);
        String[] flows = new String[]{

                "{\"type\":\"NetFlowv5\",\"src\":\"192.168.101.5\"," +
                        "\"src_net\":\"0.0.0.0/0\",\"src_net_name\":\"0.0.0.0/0\",\"dst\":\"173.194.113.68\"," +
                        "\"dst_net\":\"0.0.0.0/0\",\"dst_net_name\":\"0.0.0.0/0\",\"dst_as\":15169," +
                        "\"dst_as_name\":\"Google Inc.\",\"dst_country_code\":\"US\",\"input_snmp\":0," +
                        "\"output_snmp\":255,\"tos\":0,\"src_port\":35781,\"dst_port\":443," +
                        "\"srv_port\":443,\"tcp_flags\":\"16\",\"l4_proto\":6,\"engine_type\":0," +
                        "\"engine_id\":0,\"engine_id_name\":\"None\"," +
                        "\"sensor_ip\":\"192.168.101.1\",\"sensor_name\":\"router\",\"bytes\":52," +
                        "\"pkts\":1, \"first_switched\":" + System.currentTimeMillis() / 1000 + ", \"last_switched\":" + System.currentTimeMillis() / 1000 + "}",

                "{\"type\":\"NetFlowv5\",\"src\":\"192.168.101.5\",\"src_net\":\"0.0.0.0/0\"," +
                        "\"src_net_name\":\"0.0.0.0/0\",\"dst\":\"173.194.113.68\"," +
                        "\"dst_net\":\"0.0.0.0/0\",\"dst_net_name\":\"0.0.0.0/0\",\"dst_as\":15169," +
                        "\"dst_as_name\":\"Google Inc.\",\"dst_country_code\":\"US\",\"input_snmp\":0," +
                        "\"output_snmp\":255,\"tos\":0,\"src_port\":35781,\"dst_port\":443," +
                        "\"srv_port\":443,\"tcp_flags\":\"16\",\"l4_proto\":6,\"engine_type\":0," +
                        "\"engine_id\":0,\"engine_id_name\":\"None\",\"sensor_ip\":\"192.168.101.1\"," +
                        "\"sensor_name\":\"router\",\"bytes\":52,\"pkts\":1, \"first_switched\":" + System.currentTimeMillis() / 1000 + "," +
                        " \"last_switched\":" + (System.currentTimeMillis() / 1000 + 50) + "}",

                "{\"type\":\"NetFlowv5\",\"src\":\"192.168.101.5\",\"src_net\":\"0.0.0.0/0\"," +
                        "\"src_net_name\":\"0.0.0.0/0\",\"dst\":\"173.194.113.68\"," +
                        "\"dst_net\":\"0.0.0.0/0\",\"dst_net_name\":\"0.0.0.0/0\",\"dst_as\":15169," +
                        "\"dst_as_name\":\"Google Inc.\",\"dst_country_code\":\"US\",\"input_snmp\":0," +
                        "\"output_snmp\":255,\"tos\":0,\"src_port\":35781,\"dst_port\":443," +
                        "\"srv_port\":443,\"tcp_flags\":\"16\",\"l4_proto\":6,\"engine_type\":0," +
                        "\"engine_id\":0,\"engine_id_name\":\"None\",\"sensor_ip\":\"192.168.101.1\"," +
                        "\"sensor_name\":\"router\",\"bytes\":52,\"pkts\":1, \"first_switched\":" + System.currentTimeMillis() / 1000 + "," +
                        " \"last_switched\":" + (System.currentTimeMillis() / 1000 + 120) + "}",

                "{\"type\":\"NetFlowv5\",\"src\":\"192.168.101.5\",\"src_net\":\"0.0.0.0/0\"," +
                        "\"src_net_name\":\"0.0.0.0/0\",\"dst\":\"173.194.113.68\"," +
                        "\"dst_net\":\"0.0.0.0/0\",\"dst_net_name\":\"0.0.0.0/0\",\"dst_as\":15169," +
                        "\"dst_as_name\":\"Google Inc.\",\"dst_country_code\":\"US\",\"input_snmp\":0," +
                        "\"output_snmp\":255,\"tos\":0,\"src_port\":35781,\"dst_port\":443," +
                        "\"srv_port\":443,\"tcp_flags\":\"16\",\"l4_proto\":6,\"engine_type\":0," +
                        "\"engine_id\":0,\"engine_id_name\":\"None\",\"sensor_ip\":\"192.168.101.1\"," +
                        "\"sensor_name\":\"router\",\"bytes\":52,\"pkts\":1, \"first_switched\":" + System.currentTimeMillis() / 1000 + "," +
                        " \"last_switched\":" + (System.currentTimeMillis() / 1000 + 60) + "}"
        };


        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("test", drpc)
                .each(new Fields("args"), new MapperFunction("rb_test"), new Fields("flows"))
                .each(new Fields("flows"), new SeparateLongTimeFlowFunction(), new Fields("finalMap"))
                .project(new Fields("finalMap"))
                .each(new Fields("finalMap"), new MapToJSONFunction(), new Fields("jsonString"));

        Config conf = new Config();
        conf.put("rbDebug", true);
        conf.setMaxTaskParallelism(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testing-topology", conf, topology.build());

        for (String flow : flows) {
            String stormFlow = drpc.execute("test", flow);
            //stormFlow = stormFlow.substring(stormFlow.indexOf("{"), stormFlow.indexOf("}") + 1);
            System.out.println(stormFlow);
            //ssert.assertEquals(true, stormFlow.contains("timestamp"));
        }
    }
*/

}
