/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.util;

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
public class GetMemcachedConfig {
    public void builder() throws FileNotFoundException {

        Object object = Yaml.load(new File("/opt/rb/etc/redBorder-BI/memcached_config.yml"));
        Map<String, Object> map = (Map<String, Object>) object;
        Map<String, Object> production = (Map<String, Object>) map.get("production");
        Map<String, Object> config = null;

        /*if (_topicInt == RBEventType.EVENT) {
            config = (Map<String, Object>) production.get("events");
        } else if (_topicInt == RBEventType.FLOW) {
            config = (Map<String, Object>) production.get("traffics");
        } else if (_topicInt == RBEventType.MONITOR) {
            config = (Map<String, Object>) production.get("monitor");
        } else if (_topicInt == RBEventType.MSE) {
            config = (Map<String, Object>) production.get("location");
        }

        if (config != null) {
            _topic = config.get("datasource").toString();
            _zkConnect = config.get("zk_connect").toString();
        } else {
            Logger.getLogger(GetKafkaConfig.class.getName()).log(Level.SEVERE, null, "Topic not found");
            _zkConnect = "localhost";
        }*/
    }

}
