/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.redborder.storm.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ho.yaml.Yaml;

/**
 *
 * @author andresgomez
 */
public class GetKafkaConfig {

    int _topicInt;
    String _topic;
    String _zkConnect;

    public GetKafkaConfig() {
    }

    public GetKafkaConfig(int topic) {
        _topicInt = topic;
    }

    public void builder() throws FileNotFoundException {

        //Object object = Yaml.load(new File("/opt/rb/var/www/rb-rails/config/rbdruid_config.yml"));
        Object object = Yaml.load(new File("/Users/andresgomez/rbdruid_config.yml"));
        Map<String, Object> map = (Map<String, Object>) object;
        Map<String, Object> production = (Map<String, Object>) map.get("production");
        Map<String, Object> config = null;

        if (_topicInt == RBEventType.EVENT) {
            config = (Map<String, Object>) production.get("events");
        } else if (_topicInt == RBEventType.FLOW) {
            config = (Map<String, Object>) production.get("traffics");
        } else if (_topicInt == RBEventType.MONITOR) {
            config = (Map<String, Object>) production.get("monitor");
        }

        if (config != null) {
            _topic = config.get("datasource").toString();
            _zkConnect = config.get("zk_connect").toString();
        } else {
            Logger.getLogger(GetKafkaConfig.class.getName()).log(Level.SEVERE, null, "Topic not found");

        }
    }

    public void setTopicInt(int topic) throws FileNotFoundException {
        _topicInt = topic;
        this.builder();
    }

    public String getTopic() {
        return _topic;
    }

    public String getZkConnect() {
        return _zkConnect;
    }

    public int getTopicInt() {
        return _topicInt;
    }
}
