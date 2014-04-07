/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.metamx.tranquility.storm.BeamBolt;
import net.redborder.storm.bolt.EventBuilderBolt;
import net.redborder.storm.spout.KafkarBSpout;
import net.redborder.storm.util.GetKafkaConfig;
import net.redborder.storm.util.RBEventType;
import net.redborder.storm.util.druid.MyBeamFactoryMapMonitor;
import java.io.FileNotFoundException;
import java.util.Map;
import net.redborder.storm.spout.TwitterStreamSpout;

/**
 *
 * @author andresgomez
 */
public class CorrelationTopology {

    public static class PrinterBolt extends BaseBasicBolt {

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            //Map<String, Object> map = (Map<String, Object>) tuple.getValue(0);
            System.out.println(tuple.getValue(0).toString());
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException {

        TopologyBuilder topology = new TopologyBuilder();

        GetKafkaConfig zkConfig = new GetKafkaConfig();
        /*
        
         zkConfig.setTopicInt(RBEventType.EVENT);
         BeamBolt<Map<String, Object>> beamBoltEvent = new BeamBolt<>(new MyBeamFactoryMapEvent(
         zkConfig));
      
         topology.setSpout("kafkaSpoutEvent", new KafkarBSpout().builder(zkConfig.getZkConnect(),
         zkConfig.getTopic(), "stormKafka"));
         */
        /* zkConfig.setTopicInt(RBEventType.FLOW);
         //         BeamBolt<Map<String, Object>> beamBoltFlow = new BeamBolt<>(new MyBeamFactoryMapFlow(
         //       zkConfig));

        
         topology.setSpout("kafkaSpoutFlow", new KafkarBSpout().builder(zkConfig.getZkConnect(),
         zkConfig.getTopic(), "stormKafka"));*/


        /*
         topology.setBolt("eventBuilder", new EventBuilderBolt(RBEventType.EVENT))
         .shuffleGrouping("kafkaSpoutEvent");*/
        /*topology.setBolt("flowBuilder", new EventBuilderBolt(RBEventType.FLOW))
         .shuffleGrouping("kafkaSpoutFlow");*/
        
       
         
         zkConfig.setTopicInt(RBEventType.MONITOR);
         BeamBolt<Map<String, Object>> beamBoltMonitor = new BeamBolt<>(new MyBeamFactoryMapMonitor(
         zkConfig));

         topology.setSpout("kafkaSpoutMonitor", new KafkarBSpout().builder(zkConfig.getZkConnect(),
         zkConfig.getTopic(), "stormKafka"), 1);
         topology.setBolt("monitorBuilder", new EventBuilderBolt(RBEventType.MONITOR))
         .shuffleGrouping("kafkaSpoutMonitor");
        
  
         
        //topology.setSpout("twitterStreamSpout", new TwitterStreamSpout(),1);

        /*topology.setBolt("GeoIpBolt", new GeoIpBolt())
         .shuffleGrouping("flowBuilder");
         
         topology.setBolt("MacVendorBolt", new MacVendorBolt("/etc/objects/oui-vendors"))
         .shuffleGrouping("GeoIpBolt");*/
         topology.setBolt("druidBolt", beamBoltMonitor, 1)
                .shuffleGrouping("monitorBuilder");
        topology.setBolt("PrintBuilder", new PrinterBolt())
                .shuffleGrouping("monitorBuilder");

        if (args[0].equalsIgnoreCase("local")) {
            Config conf = new Config();
                        String CONSUMER_KEY = "twitter.consumerKey";
            String CONSUMER_SECRET = "twitter.consumerSecret";
            String TOKEN = "twitter.token";
            String TOKEN_SECRET = "twitter.tokenSecret";
            String QUERY = "twitter.query";
            
            conf.put(CONSUMER_KEY, "Vkoyw2Bwgk13RFaTyJlYQ");
            conf.put(CONSUMER_SECRET, "TkW74gdR764dH6lOkD3cKSwGLMKy7xrA9s7ZCZsqRno");
            conf.put(TOKEN, "154536310-Yxg7DqA6mg982MSxG2peKa6TIUf00loFJnVMwOaP");
            conf.put(TOKEN_SECRET, "oG5JIcg1CKCDNQwqIVrt1RVR2bqPWZ91DUJXEYefnjCkX");
            conf.put(QUERY,"Dignidad22M");
            conf.setMaxTaskParallelism(1);
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Topology", conf, topology.createTopology());


            
            
            Utils.sleep(600000);
            cluster.killTopology("Redborder-Topology");
            cluster.shutdown();

        } else if (args[0].equalsIgnoreCase("cluster")) {

            //Creacion y configuracion cluster storm.
            Config conf = new Config();
            StormSubmitter.submitTopology("Redborder-Topology", conf, topology.createTopology());
        }
    }
}
