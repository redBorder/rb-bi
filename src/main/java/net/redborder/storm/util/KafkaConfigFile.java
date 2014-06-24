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

    private final String CONFIG_FILE_PATH = "/opt/rb/etc/redBorder-BI/zk_config.yml";
    private Map<String, Object> _production;
    private Map<String, Object> _general;
    private List<String> _availableTopics;
    private boolean _debug = false;

    /**
     * Constructor
     */
    public KafkaConfigFile(boolean debug) {
        _debug = debug;

        try {
            Map<String, Object> map = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));

            /* Production Config */
            _production = (Map<String, Object>) map.get("production");

            for (Object value : _production.values()) {
                Map<String, Object> config = (Map<String, Object>) value;
                _availableTopics.add(value.toString());

                if (_debug) {
                    System.out.println("Select section: " + value.toString());
                    System.out.println("  - inputTopic: [" + config.get("input_topic") + "]");
                    System.out.println("  - outputTopic: [" + config.get("output_topic") + "]");
                    System.out.println("  - zkConnect: [" + config.get("zk_connect") + "]");
                }
            }

            /* General Config */
            _general = (Map<String, Object>) map.get("general");
        } catch (FileNotFoundException e) {
            Logger.getLogger(KafkaConfigFile.class.getName()).log(Level.SEVERE, "kafka config file not found");
        }
    }

    /**
     * Getter.
     *
     * @param section Section to read from the config file
     * @param property Property to read from the section
     * @return Property read
     */
    public String get(String section, String property) {
        Map<String, Object> map = (Map<String, Object>) _production.get(section);
        String result = null;

        if (map != null) {
            result = (String) map.get(property);
        }

        return result;
    }

    public boolean contains(String section) {
        return _production.containsKey(section);
    }

    /**
     * Getter.
     *
     * @param section Section to read from the config file
     * @return name topic
     */
    public String getTopic(String section) {
        return get(section, "input_topic");
    }

    /**
     * Getter.
     *
     * @param section Section to read from the config file
     * @return zookeeper host.
     */
    public String getZkHost(String section) {
        return get(section, "zk_connect");
    }

    /**
     * Getter.
     *
     * @param section Section to read from the config file
     * @return output topic name
     */
    public String getOutputTopic(String section) {
        return get(section, "output_topic");
    }

    public boolean getOverwriteCache(String section) {
        String ret = get(section, "overwrite_cache");
        return ret != null && ret.equals("true");
    }

    public boolean darklistIsEnabled() {
        String ret = (String) _general.get("blacklist");
        return ret != null && ret.equals("true");
    }

    public List<String> getAvailableTopics() {
        return _availableTopics;
    }
}
