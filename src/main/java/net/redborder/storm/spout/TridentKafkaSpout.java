/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

/**
 * Create a tridentKafkaSpout.
 * @author andresgomez
 */
public class TridentKafkaSpout {
    
    /**
     * Build the trindetKafkaSpout.
     * @param zkHost Zookeeper IP.
     * @param topic Topic of kafka.
     * @param groupid Group to do commit in zookeeper.
     * @return Trindet spout of kafka.
     */
    public TransactionalTridentKafkaSpout builder(String zkHost, String topic, String groupid){
       TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(new ZkHosts(zkHost),
                topic, groupid);
       
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
        
        return kafkaSpout;
    }
    
}
