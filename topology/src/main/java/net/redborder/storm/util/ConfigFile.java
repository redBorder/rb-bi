/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util;

import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author andresgomez
 */
public class ConfigFile {

    private final String CONFIG_FILE_PATH = "/opt/rb/etc/rb-bi/config.yml";
    private Map<String, Object> _sections;
    private Map<String, Object> _general;
    private List<String> _availableTopics;

    /**
     * Constructor
     */
    public ConfigFile(boolean debug) {
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


                if (debug) {
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

    public <T> T get(String section, String property) {
        Map<String, Object> map = (Map<String, Object>) _sections.get(section);
        T result = null;

        if (map != null) {
            result = (T) map.get(property);
        }

        return result;
    }

    /**
     * Getter.
     *
     * @param property Property to read from the general section
     * @return Property read
     */

    public <T> T getFromGeneral(String property) {
        T ret = null;

        if(_general != null) {
            ret = (T) _general.get(property);
        }

        return ret;
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
     * @return List of _topics listed in the config file
     */
    public List<String> getAvailableTopics() {
        return _availableTopics;
    }
}
