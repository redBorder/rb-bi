/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import java.io.FileNotFoundException;
import net.redborder.storm.util.KafkaConfigFile;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

/**
 * Create a tridentKafkaSpout.
 *
 * @author andresgomez
 */
public class TridentKafkaSpout {

    TridentKafkaConfig _kafkaConfig;

    /**
     * Constructor
     *
     * @param config Config file to read properties from
     * @param section Section of the kafka config file to read properties from.
     */
    public TridentKafkaSpout(KafkaConfigFile config, String section) {
        _kafkaConfig = new TridentKafkaConfig(new ZkHosts(config.getZkHost(section)), config.getTopic(section), "stormKafka");
        _kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        _kafkaConfig.bufferSizeBytes = 1024 * 1024 * 4;
        _kafkaConfig.fetchSizeBytes = 1024 * 1024 * 4;
        _kafkaConfig.forceFromStart = false;
    }

    /**
     * Build the tridentKafkaSpout.
     *
     * @return Trident spout of kafka.
     */
    public TransactionalTridentKafkaSpout builder() {
        //Logger.getLogger(KafkaConfigFile.class.getName()).log(Level.INFO, "Reading from topic " + _configFile.getTopic());
        return new TransactionalTridentKafkaSpout(_kafkaConfig);
    }

}
