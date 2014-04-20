/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import java.io.FileNotFoundException;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    KafkaConfigFile _configFile;
    TridentKafkaConfig _kafkaConfig;

    /**
     * Constructor
     *
     * @param section Section of the kafka config file to read properties from.
     * @throws java.io.FileNotFoundException
     */
    public TridentKafkaSpout(String section) throws FileNotFoundException {
        _configFile = new KafkaConfigFile(section);
        _kafkaConfig = new TridentKafkaConfig(new ZkHosts(_configFile.getZkHost()), _configFile.getTopic(), "stormKafka");
        _kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        _kafkaConfig.forceFromStart = false;
    }

    /**
     * Build the trindetKafkaSpout.
     *
     * @return Trident spout of kafka.
     */
    public TransactionalTridentKafkaSpout builder() {
        Logger.getLogger(KafkaConfigFile.class.getName()).log(Level.INFO, "Reading from topic " + _configFile.getTopic());
        return new TransactionalTridentKafkaSpout(_kafkaConfig);
    }

}
