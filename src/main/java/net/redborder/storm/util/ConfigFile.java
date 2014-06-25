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
public class ConfigFile {

    private final String CONFIG_FILE_PATH = "/opt/rb/etc/redBorder-BI/zk_config.yml";
    private Map<String, Object> _sections;
    private Map<String, Object> _general;
    private List<String> _availableTopics;

    /**
     * Constructor
     */
    public ConfigFile() {
        _availableTopics = new ArrayList<>();

        try {
            Map<String, Object> map = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));

            /* Production Config */
            _sections = (Map<String, Object>) map.get("sections");

            for (Object value : _sections.values()) {
                Map<String, Object> config = (Map<String, Object>) value;

                _availableTopics.add(config.get("input_topic").toString());
                if (config.get("output_topic") != null) {
                    _availableTopics.add(config.get("output_topic").toString());
                }


                if (ConfigData.debug) {
                    System.out.println("Select section: " + value.toString());
                    System.out.println("  - inputTopic: [" + config.get("input_topic") + "]");
                    System.out.println("  - outputTopic: [" + config.get("output_topic") + "]");
                    System.out.println("  - zkConnect: [" + config.get("zk_connect") + "]");
                }
            }

            /* General Config */
            _general = (Map<String, Object>) map.get("general");
        } catch (FileNotFoundException e) {
            Logger.getLogger(ConfigFile.class.getName()).log(Level.SEVERE, "config file not found");
        }
    }

    /**
     * Getter.
     *
     * @param section  Section to read from the config file
     * @param property Property to read from the section
     * @return Property read
     */

    public String get(String section, String property) {
        Map<String, Object> map = (Map<String, Object>) _sections.get(section);
        String result = null;

        if (map != null) {
            result = (String) map.get(property);
        }

        return result;
    }

    /**
     * Getter.
     *
     * @param section Section to check existence
     * @return true if the section exists in the config file
     */
    public boolean contains(String section) {
        return _sections.containsKey(section);
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
     * @return output topic name
     */
    public String getOutputTopic(String section) {
        return get(section, "output_topic");
    }

    /**
     * Getter.
     *
     * @param section Section to read from the config file
     * @return true if must overwrite cache for that section
     */
    public boolean getOverwriteCache(String section) {
        String ret = get(section, "overwrite_cache");
        return ret != null && ret.equals("true");
    }

    /**
     * Getter.
     *
     * @return zookeeper host.
     */
    public String getZkHost() {
        String ret = null;

        if(_general != null) {
            ret = (String) _general.get("zk_connect");
        }

        return ret;
    }

    /**
     * Getter.
     *
     * @return darklist enabled or not
     */
    public boolean darklistIsEnabled() {
        if(_general != null) {
            String ret = (String) _general.get("blacklist");
            return ret != null && ret.equals("true");
        } else {
            return false;
        }

    }

    /**
     * Getter.
     *
     * @return List of riak servers
     */
    public List<String> getRiakServers() {
        List<String> servers = (List<String>) _general.get("riak_servers");
        List<String> riakServers;

        if (servers != null) {
            riakServers = servers;
        } else {
            Logger.getLogger(ConfigFile.class.getName()).log(Level.SEVERE, "No riak servers on config file");
            riakServers = new ArrayList<>();
            riakServers.add("localhost");

        }

        return riakServers;
    }

    /**
     * Getter.
     *
     * @return Tranquility replication factor
     */
    public Integer getTranquilityReplication() {
        Integer ret = null;

        if(_general != null) {
            ret = (Integer) _general.get("tranquility_replication");
        }

        return ret;
    }

    /**
     * Getter.
     *
     * @return List of _topics listed in the config file
     */
    public List<String> getAvailableTopics() {
        return _availableTopics;
    }
}
