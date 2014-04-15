package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import net.redborder.storm.topologies.RedBorderTopologies;
import net.redborder.storm.util.CreateConfig;
import java.io.FileNotFoundException;
import storm.trident.TridentTopology;


public class RedBorderTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {

        String topologyName = "redBorder-Topology";
        if (args.length != 1) {
            System.out.println("./storm jar {name_jar} {main_class} {local|cluster}");
        } else {
            RedBorderTopologies topologies = new RedBorderTopologies();             
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
