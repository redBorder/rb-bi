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
public class KafkaConfigFile {

    String _topic;
    String _zkHost;
    Map<String, Object> _data;
    
    final String CONFIG_FILE_PATH = "/opt/rb/etc/redBorder-BI/zk_config.yml";
    // final String CONFIG_FILE_PATH = "/Users/andresgomez/rbdruid_config.yml"

    /**
     * Constructor.
     * 
     * @throws FileNotFoundException
     */
    public KafkaConfigFile() throws FileNotFoundException {
        _zkHost = "localhost";
        Map<String, Object> map = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));
        _data = (Map<String, Object>) map.get("production");
    }

    /**
     * Constructor
     * @param section Section to read from the config file
     * 
     * @throws FileNotFoundException
     */
    public KafkaConfigFile(String section) throws FileNotFoundException {
        this();
        this.setSection(section);
    }

    /**
     * Get the zookeeper setting of the topic indicated.
     * @param section Section to read from the config file
     */
    public final void setSection(String section) {
        Map<String, Object> config = (Map<String, Object>) _data.get(section);

        if (config != null) {
            _topic = config.get("datasource").toString();
            _zkHost = config.get("zk_connect").toString();
        } else {
            Logger.getLogger(KafkaConfigFile.class.getName()).log(Level.SEVERE, "Section not found");
            _zkHost = "localhost";
        }
    }


    /**
     * Getter.
     *
     * @return name topic
     */
    public String getTopic() {
        return _topic;
    }

    /**
     * Getter.
     *
     * @return zookeeper host.
     */
    public String getZkHost() {
        return _zkHost;
    }
}