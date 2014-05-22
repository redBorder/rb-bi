/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util;

import java.io.IOException;
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
import backtype.storm.Config;

/**
 *
 * @author andresgomez
 */
public class ConfigData {

    CuratorFramework client;
    Config conf;

    public ConfigData(KafkaConfigFile zk) {
        conf = new Config();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(zk.getZkHost(), retryPolicy);
        client.start();
    }

    public int getKafkaPartitions(String topic) {
        int partitions;

        List<String> partitionsList;
        try {
            partitionsList = client.getChildren().forPath("/brokers/topics/" + topic + "/partitions");
            partitions = partitionsList.size();
        } catch (Exception ex) {
            Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No partitions found. Default: 2", ex);
            partitions = 2;
        }

        return partitions;
    }

    public int getWorkers() {
        int workers;

        List<String> workersList;
        try {
            workersList = client.getChildren().forPath("/storm/supervisors");
            workers = workersList.size();
        } catch (Exception ex) {
            Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No supervisor found. Default: 1", ex);
            workers = 1;
        }

        return workers;
    }

    public int getMiddleManagerCapacity() {
        int middleManagers = 0;
        
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
            Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No supervisor found. Default: 1", ex);
        }

        if (middleManagers == 0) {
            middleManagers = 1;
        }

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
            conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 10);
        }
        return conf;
    }

}
