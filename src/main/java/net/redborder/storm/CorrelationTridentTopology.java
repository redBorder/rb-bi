package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Values;
import net.redborder.storm.topologies.TridentRedBorderTopologies;
import net.redborder.storm.util.CreateConfig;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import storm.trident.TridentTopology;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CorrelationTridentTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {

        String topologyName = "redBorder-Topology";
        if (args.length != 1) {
            System.out.println("./storm jar {name_jar} {main_class} {local|cluster}");
        } else {
            TridentRedBorderTopologies topologies = new TridentRedBorderTopologies();             
            TridentTopology topology = topologies.Test();         

            if (args[0].equalsIgnoreCase("local")) {
                Config conf = new CreateConfig(args[0]).makeConfig();

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, conf, topology.build());

                //Utils.sleep(1000000);
                //cluster.killTopology(topologyName);
                //cluster.shutdown();

            } else if (args[0].equalsIgnoreCase("cluster")) {

                Config conf = new CreateConfig(args[0]).makeConfig();
                StormSubmitter.submitTopology(topologyName, conf, topology.build());
                System.out.println("Topology: " + topologyName  + " uploaded successfully.");
            }
        }
    }
}
