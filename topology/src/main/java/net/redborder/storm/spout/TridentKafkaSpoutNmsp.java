/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import net.redborder.storm.util.ConfigData;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

/**
 * Create a tridentKafkaSpout.
 *
 * @author andresgomez
 */
public class TridentKafkaSpoutNmsp {

    TridentKafkaConfig _kafkaConfig;

    /**
     * Constructor
     *
     * @param config Config file to read properties from
     * @param section Section of the kafka config file to read properties from.
     */
    public TridentKafkaSpoutNmsp(ConfigData config, String section) {
        _kafkaConfig = new TridentKafkaConfig(new ZkHosts(config.getZkHost()), config.getTopic(section), "stormKafka");
        _kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        _kafkaConfig.bufferSizeBytes = config.getFetchSizeKafkaNmsp();
        _kafkaConfig.fetchSizeBytes = config.getFetchSizeKafkaNmsp();
        _kafkaConfig.forceFromStart = false;
    }

    /**
     * Build the tridentKafkaSpout.
     *
     * @return Trident spout of kafka.
     */
    public OpaqueTridentKafkaSpout builder() {
        return new OpaqueTridentKafkaSpout(_kafkaConfig);
    }

}
