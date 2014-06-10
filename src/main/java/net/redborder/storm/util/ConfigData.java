/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util;

import backtype.storm.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.redborder.storm.function.MapperFunction;
import net.redborder.storm.function.ProducerKafkaFunction;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author andresgomez
 */
public class ConfigData {

    CuratorFramework client;
    Config conf;
    List<String> topics;
    Map<String, Integer> kafkaPartitions;
    Integer numWorkers;
    int middleManagers;

    public ConfigData(KafkaConfigFile kafkaConfig) {
        conf = new Config();
        kafkaPartitions = new HashMap<>();
        this.topics = kafkaConfig.getAvaibleTopics();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(kafkaConfig.getZkHost(), retryPolicy);
        client.start();
        initKafkaPartitions();
        initWorkers();
        initMiddleManagerCapacity();
        client.close();
    }

    private void initKafkaPartitions() {
        List<String> partitionsList;

        for (String topic : topics) {
            try {
                partitionsList = client.getChildren().forPath("/brokers/topics/" + topic + "/partitions");
                kafkaPartitions.put(topic, partitionsList.size());
            } catch (Exception ex) {
                Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No partitions found. Default: 2", ex);
                kafkaPartitions.put(topic, 2);
            }
        }

    }

    public int getKafkaPartitions(String topic) {
        return kafkaPartitions.get(topic);
    }

    private void initWorkers() {

        List<String> workersList;
        try {
            workersList = client.getChildren().forPath("/storm/supervisors");
            numWorkers = workersList.size();
        } catch (Exception ex) {
            Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No supervisor found. Default: 1", ex);
            numWorkers = 1;
        }

    }

    public int getWorkers() {
        return numWorkers;
    }

    private void initMiddleManagerCapacity() {
        
        try {
            List<String> middleManagersList = client.getChildren().forPath("/druid/indexer/announcements");

            for (String middleManager : middleManagersList) {
                String jsonString = null;

                try {
                    jsonString = new String(client.getData().forPath("/druid/indexer/announcements/" + middleManager), "UTF-8");
                } catch (Exception ex) {
                    Logger.getLogger(ProducerKafkaFunction.class.getName()).log(Level.SEVERE, null, ex);
                }

                if (jsonString != null) {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> json;

                    try {
                        json = mapper.readValue(jsonString, Map.class);
                        middleManagers = (Integer) json.get("capacity") + middleManagers;

                    } catch (IOException | NullPointerException ex) {
                        Logger.getLogger(MapperFunction.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", ex);
                    }
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No middle managers found. Default: 1", ex);
        }

        if (middleManagers == 0) {
            middleManagers = 1;
        }
    }

    public int getMiddleManagerCapacity() {
        return middleManagers;
    }

    /**
     * Getter.
     *
     * @param _mode
     * @return the config of storm topology.
     */
    public Config getConfig(String _mode) {
        if (_mode.equals("local")) {
            conf.setMaxTaskParallelism(1);
            conf.setDebug(false);
        } else if (_mode.equals("cluster")) {
            conf.put(Config.TOPOLOGY_WORKERS, getWorkers());
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5);
        }
        return conf;
    }

}
