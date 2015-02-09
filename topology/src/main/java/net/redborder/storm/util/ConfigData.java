/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util;

import backtype.storm.Config;
import net.redborder.metrics.KafkaConsumerMonitorMetrics;
import net.redborder.metrics.Metrics2KafkaProducer;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author andresgomez
 */
public class ConfigData {

    public boolean debug;
    private CuratorFramework _curator;
    private Config _conf;
    private ConfigFile _configFile;
    private List<String> _topics;
    private Map<String, Integer> _kafkaPartitions;
    private Map<String, Integer> _tranquilityPartitions;
    private Integer _numWorkers;
    private int _middleManagers;
    private String _zookeeper;

    public ConfigData() {
        _conf = new Config();
        _configFile = new ConfigFile(debug);
        _kafkaPartitions = new HashMap<>();
        _topics = _configFile.getAvailableTopics();
        _tranquilityPartitions = new HashMap<>();
        _zookeeper = getZkHost();
        debug = false;
        getZkData();
        getTranquilityPartitions();
    }

    private void getZkData() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        _curator = CuratorFrameworkFactory.newClient(_zookeeper, retryPolicy);
        _curator.start();
        initKafkaPartitions();
        initWorkers();
        initMiddleManagerCapacity();
        _curator.close();
    }


    private void initKafkaPartitions() {
        List<String> partitionsList;

        for (String topic : _topics) {
            try {
                partitionsList = _curator.getChildren().forPath("/brokers/topics/" + topic + "/partitions");
                _kafkaPartitions.put(topic, partitionsList.size());
            } catch (Exception ex) {
                Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No partitions found. Default: 2", ex);
                _kafkaPartitions.put(topic, 2);
            }
        }

    }

    public int getKafkaPartitions(String topic) {
        Integer ret = _kafkaPartitions.get(topic);
        return ret != null ? ret : 0;
    }


    private void initWorkers() {

        Integer workers = getNumWorkers();

        if(workers==null) {
            List<String> workersList;

            try {
                workersList = _curator.getChildren().forPath("/storm/supervisors");
                _numWorkers = workersList.size();
            } catch (Exception ex) {
                Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No supervisor found. Default: 1", ex);
                _numWorkers = 1;
            }
        }else{
            _numWorkers = workers;
        }
    }

    public int getWorkers() {
        return _numWorkers;
    }

    public boolean getCorrealtionEnabled() {
        Boolean ret = _configFile.getFromGeneral("correlation");
        return ret != null && ret;
    }

    private void initMiddleManagerCapacity() {
        int servers = 0;
        int minimum = 99999;
        int total = 0;
        int capacity;

        try {
            List<String> middleManagersList = _curator.getChildren().forPath("/druid/indexer/announcements");

            for (String middleManager : middleManagersList) {
                String jsonString = new String(_curator.getData().forPath("/druid/indexer/announcements/" + middleManager), "UTF-8");
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> json = mapper.readValue(jsonString, Map.class);
                Integer nodeCapacity = (Integer) json.get("capacity");

                if (minimum > nodeCapacity) minimum = nodeCapacity;
                total = nodeCapacity + total;
                servers++;
            }
        } catch (IOException | NullPointerException ex) {
            Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", ex);
        } catch (Exception ex) {
            Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE, "No middle managers found, maybe use kafka to kafka. Default: 1");
        }

        if (servers == 0) {
            servers = 1;
            total = minimum = 3;
        }

        if (tranquilityReplication() == 1) {
            capacity = total;
        } else {
            capacity = minimum * servers;
        }

        _middleManagers = capacity;
    }

    public int getMiddleManagerCapacity() {
        return _middleManagers;
    }

    public String getDbUri(){
        return _configFile.getFromGeneral("db_uri");
    }

    public String getDbUser(){
        return _configFile.getFromGeneral("db_user");
    }

    public String getDbPass(){
        return _configFile.getFromGeneral("db_pass");
    }

    public int getMaxRows() {
        Integer maxRows = _configFile.getFromGeneral("max_rows");
        return maxRows != null ? maxRows : 120000;
    }

    public int getEmitBatchInterval(){
        Integer emit = _configFile.getFromGeneral("emit_batch_interval");
        return emit != null ? emit : 1000;
    }

    public int getMaxSpoutPending(){
        Integer max_spout = _configFile.getFromGeneral("max_spout_pending");
        return max_spout != null ? max_spout : 5;
    }

    public Config setConfig(String mode) {
        if (mode.equals("local")) {
            _conf.setMaxTaskParallelism(1);
            _conf.setDebug(false);
        } else if (mode.equals("cluster")) {
            _conf.put(Config.TOPOLOGY_WORKERS, getWorkers());
            _conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, getMaxSpoutPending());
            _conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, getEmitBatchInterval());
            _conf.put("rbDebug", debug);
            Boolean hash_mac = _configFile.getFromGeneral("hash_mac");

            if (hash_mac != null)
                _conf.put("hash_mac", hash_mac);
            else
                _conf.put("hash_mac", false);

            /*  Metrics  */
            Map<String, Object> zkMetricsConf = new HashMap<>();
            zkMetricsConf.put("zookeeper", _zookeeper);
            _conf.registerMetricsConsumer(KafkaConsumerMonitorMetrics.class, zkMetricsConf, 1);


            if (getMetrics()) {

                Map<String, Object> functionMetricsConf = new HashMap<>();
                List<String> metrics = new ArrayList<>();

                metrics.add("throughput");
                functionMetricsConf.put("zookeeper", _zookeeper);
                functionMetricsConf.put("metrics", metrics);
                functionMetricsConf.put("topic", "rb_monitor");

                _conf.registerMetricsConsumer(Metrics2KafkaProducer.class, functionMetricsConf, 1);

            }
        }

        return _conf;
    }

    public double getTranquilityBackup(){
        Double percent = _configFile.getFromGeneral("tranquility_backup_percent");
        return percent == null ? 0.75 : (1-percent);
    }

    private Integer getNumWorkers(){
        Integer num = _configFile.getFromGeneral("num_workers");
        return num;
    }

    public Integer getParallelismFactor(){
        Integer parallelismFactor = _configFile.getFromGeneral("parallelism_factor");
        return parallelismFactor != null ? parallelismFactor: 2;
    }

    public Integer getFetchSizeKafka(){
        Integer fetchsize = _configFile.getFromGeneral("kafka_fetchsize");
        return fetchsize != null ? fetchsize : 1024 * 1024 * 8;
    }


    public void getTranquilityPartitions() {
        Double capacityd = getMiddleManagerCapacity()*getTranquilityBackup();
        int replication = tranquilityReplication();
        int divider = 0;
        double slot;

        if (tranquilityEnabled("traffics")) divider = divider + 2;
        if (tranquilityEnabled("events")) divider = divider + 2;
        if (tranquilityEnabled("monitor")) capacityd = capacityd - 4;

        if (divider > 0) {
            if (capacityd >= divider * replication * 2) {
                slot =  capacityd / (replication * 2)/ divider;
                _tranquilityPartitions.put("traffics", (int)Math.floor(slot*2));
                _tranquilityPartitions.put("events", (int)Math.floor(slot*2));
                _tranquilityPartitions.put("monitor", 1);
            } else {
                Logger.getLogger(ConfigData.class.getName()).log(Level.SEVERE,
                        "Not enough middle manager capacity");
            }
        }
    }

    public Integer getTranquilityLimit(String section) {
        return _configFile.get(section, "tranquility_limit");
    }

    public int tranquilityPartitions(String section) {
        Integer partitions;
        Integer partitionLimit = getTranquilityLimit(section);
        if (partitionLimit != null) {
            partitions = partitionLimit;
        } else {
            partitions = _tranquilityPartitions.get(section);
        }
        return partitions == null ? 1 : partitions;
    }

    public int tranquilityReplication() {
        return _configFile.getFromGeneral("tranquility_replication");
    }

    public boolean contains(String section) {
        return _configFile.contains(section);
    }

    public String getTopic(String section) {
        return _configFile.get(section, "input_topic");
    }

    public String getOutputTopic(String section) {
        return _configFile.get(section, "output_topic");
    }

    public boolean tranquilityEnabled(String section) {
        return _configFile.contains(section) && getOutputTopic(section) == null;
    }

    public boolean getOverwriteCache(String section) {
        String ret = _configFile.get(section, "overwrite_cache");
        return ret != null && ret.equals("true");
    }

    public boolean getMetrics() {
        Boolean ret = _configFile.getFromGeneral("metrics");
        return ret != null && ret;
    }

    public String getZkHost() {
        return _configFile.getFromGeneral("zk_connect");
    }

    public boolean darklistIsEnabled() {
        Boolean ret = _configFile.getFromGeneral("darklist");
        return ret != null && ret;
    }

    public boolean nmspLocationStatsEnabled() {
        Boolean ret = _configFile.getFromGeneral("nmsp_location_stats");
        return ret != null && ret;
    }

    public boolean mseLocationStatsEnabled(){
        Boolean ret =  _configFile.getFromGeneral("mse_location_stats");
        return ret != null && ret;
    }

    public Boolean getMacLocallyAdministeredEnable(){
        Boolean enable = _configFile.getFromGeneral("filter_mac_locally_administered");
        return enable != null ? enable : true;
    }

    public Map<String, Object> getGridGainConfig() {
        Map<String, Object> ret = _configFile.getFromGeneral("gridgain");
        return ret;
    }

    public List<String> getGridGainServers() {
        Map<String, Object> gridGainConfig = _configFile.getFromGeneral("gridgain");
        return (List<String>) gridGainConfig.get("servers");
    }

    public String getGridGainMulticast() {
        Map<String, Object> gridGainConfig = _configFile.getFromGeneral("gridgain");
        return (String) gridGainConfig.get("multicast");
    }

    public List<String> getEnrichs() {
        List<String> enrichs = new ArrayList<>();

        if (darklistIsEnabled()) enrichs.add("darklist");
        if (_configFile.contains("radius")) enrichs.add("radius");
        if (_configFile.contains("mobile")) enrichs.add("mobile");
        if (_configFile.contains("location")) enrichs.add("location");
        if (_configFile.contains("trap")) enrichs.add("trap");
        if (_configFile.contains("nmsp")) enrichs.add("nmsp");

        return enrichs;
    }

    public String getCacheType() {
        Map<String, Object> ret = (Map<String, Object>) _configFile.getFromGeneral("cache");
        String cacheType = (String) ret.get("type");
        return cacheType == null ? "gridgain" : cacheType;
    }

    public List<String> getRiakServers() {
        Map<String, Object> riakOpts = (Map<String, Object>) _configFile.getFromGeneral("riak");
        List<String> riakServers = null;

        if (riakOpts != null) {
            List<String> servers = (List<String>) riakOpts.get("servers");

            if (servers != null) {
                riakServers = servers;
            } else {
                Logger.getLogger(ConfigFile.class.getName()).log(Level.SEVERE, "No riak servers on config file");
                riakServers = new ArrayList<>();
                riakServers.add("localhost");
            }
        }

        return riakServers;
    }

    public List<String> getMemcachedServersAsString() {
        Map<String, Object> memcachedOpts = (Map<String, Object>) _configFile.getFromGeneral("memcached");
        List<String> servers = null;
        if (memcachedOpts != null) {
            servers = (List<String>) memcachedOpts.get("servers");

        } else {
            Logger.getLogger(ConfigFile.class.getName()).log(Level.SEVERE, "No riak servers on config file");
            servers = new ArrayList<>();
            servers.add("localhost:11211");
        }
        return servers;
    }

    public List<InetSocketAddress> getMemcachedServers() {
        Map<String, Object> memcachedOpts = (Map<String, Object>) _configFile.getFromGeneral("memcached");
        List<InetSocketAddress> memcachedServers = new ArrayList<>();

        if (memcachedOpts != null) {
            List<String> servers = (List<String>) memcachedOpts.get("servers");

            if (servers != null) {
                for (String server : servers) {
                    String[] inetAddress = server.split(":");
                    memcachedServers.add(new InetSocketAddress(inetAddress[0], Integer.valueOf(inetAddress[1])));
                }
            } else {
                Logger.getLogger(ConfigFile.class.getName()).log(Level.SEVERE, "No riak servers on config file");
                memcachedServers = new ArrayList<>();
                memcachedServers.add(new InetSocketAddress("localhost", 11211));
            }
        }

        return memcachedServers;
    }
}