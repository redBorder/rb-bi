/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
/**
 *
 * @author andresgomez
 */
public class KafkarBSpout {
       public KafkaSpout builder(String zkHost, String topic, String groupid){
       SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(zkHost),
                topic, "/kafka/brokers", groupid);

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        return kafkaSpout;
    }
}
