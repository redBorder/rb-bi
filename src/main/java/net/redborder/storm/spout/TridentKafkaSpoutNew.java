/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import java.io.FileNotFoundException;
import net.redborder.storm.util.KafkaConfigFile;
import nl.minvenj.nfi.storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

/**
 * Create a tridentKafkaSpout.
 *
 * @author andresgomez
 */
public class TridentKafkaSpoutNew {

    TridentKafkaConfig _kafkaConfig;
    
    String _topic;
    String _zkConnect;
    String _groupId;

    /**
     * Constructor
     *
     * @param config
     * @param section Section of the kafka config file to read properties from.
     * @throws java.io.FileNotFoundException
     */
    public TridentKafkaSpoutNew(KafkaConfigFile config, String section) throws FileNotFoundException {
        config.setSection(section);
        _topic = config.getTopic();
        _zkConnect = config.getZkHost();
        _groupId = "rb-storm-consumer";
    }

    /**
     * Build the trindetKafkaSpout.
     *
     * @return Trident spout of kafka.
     */
    public KafkaSpout builder() {
        //Logger.getLogger(KafkaConfigFile.class.getName()).log(Level.INFO, "Reading from topic " + _configFile.getTopic());
        return new KafkaSpout(_zkConnect, _topic, _groupId);
    }

}
