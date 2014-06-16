/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ho.yaml.Yaml;

/**
 * @author andresgomez
 */
public class KafkaConfigFile {

    String _topic;
    String _zkHost;
    String _outputTopic;
    Map<String, Object> _production;
    Map<String, Object> _general;
    List<String> avaibleTopics;
    boolean overwriteCache;
    boolean debug = false;

    boolean blacklist;

    final String CONFIG_FILE_PATH = "/opt/rb/etc/redBorder-BI/zk_config.yml";

    /**
     * Constructor.
     *
     * @throws FileNotFoundException
     */
    public KafkaConfigFile(boolean debug) throws FileNotFoundException {
        _zkHost = "localhost";
        this.debug = debug;
        Map<String, Object> map = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));

        /*
            Production Config.
         */
        _production = (Map<String, Object>) map.get("production");
        avaibleTopics = new ArrayList<>();
        for (Object value : _production.values()) {
            Map<String, Object> config = (Map<String, Object>) value;
            avaibleTopics.add(config.get("input_topic").toString());
            if (config.get("output_topic") != null) {
                avaibleTopics.add(config.get("output_topic").toString());
            }
            _zkHost = config.get("zk_connect").toString();
        }

        /*
            General Config.
         */
        _general = (Map<String, Object>) map.get("general");

        if(_general!=null) {
            if (_general.containsKey("blacklist")) {
                blacklist = (boolean) _general.get("blacklist");
            } else {
                blacklist = false;
            }
        }else {
            Logger.getLogger(KafkaConfigFile.class.getName()).log(Level.SEVERE, "general not found in YAML");
        }


    }

    /**
     * Constructor
     *
     * @param section Section to read from the config file
     * @throws FileNotFoundException
     */
    public KafkaConfigFile(String section, boolean debug) throws FileNotFoundException {
        this(debug);
        this.setSection(section);
    }

    /**
     * Get the zookeeper setting of the topic indicated.
     *
     * @param section Section to read from the config file
     */
    public final void setSection(String section) {
        Map<String, Object> config = (Map<String, Object>) _production.get(section);
        if (config != null) {
            Object outputTopic = config.get("output_topic");
            _topic = config.get("input_topic").toString();
            _zkHost = config.get("zk_connect").toString();

            if (config.containsKey("overwrite_cache")) {
                overwriteCache = (boolean) config.get("overwrite_cache");
            } else {
                overwriteCache = true;
            }

            if (outputTopic != null) {
                _outputTopic = outputTopic.toString();
            } else {
                _outputTopic = null;
            }

            if (debug) {
                System.out.println("Select section: " + section);
                System.out.println("  - inputTopic: [" + _topic + "]");
                System.out.println("  - outputTopic: [" + _outputTopic + "]");
                System.out.println("  - zkConnect: [" + _zkHost + "]");
            }
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

    /**
     * Getter.
     *
     * @return output topic name
     */
    public String getOutputTopic() {
        return _outputTopic;
    }

    public List<String> getAvaibleTopics() {
        return avaibleTopics;
    }

    public boolean getOverwriteCache(String section) {
        boolean debugAux = debug;
        debug = false;
        this.setSection(section);
        debug = debugAux;
        return overwriteCache;
    }

    public boolean getDarkList() {
        return blacklist;
    }
}
