/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.redborder.storm.trident.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

/**
 *
 * @author andresgomez
 */
public class TrindetKafkaSpout {
    
    public TransactionalTridentKafkaSpout builder(String zkHost, String topic, String groupid){
       TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(new ZkHosts(zkHost),
                topic, groupid);
       
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
        
        return kafkaSpout;
    }
    
}
