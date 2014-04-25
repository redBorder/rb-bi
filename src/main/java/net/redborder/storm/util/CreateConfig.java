/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util;

import backtype.storm.Config;

/**
 * Create the configuration that was used in the storm's topology.
 *
 * @author andresgomez
 */
public class CreateConfig {

    private Config conf;
    private String _mode;

    /**
     * Constructor - Default mode "cluster".
     */
    public CreateConfig() {
        _mode = "cluster";
        conf = new Config();
    }

    /**
     * Constructor.
     *
     * @param mode Mode to run the topology {local|cluster}.
     */
    public CreateConfig(String mode) {
        _mode = mode;
        conf = new Config();
    }

    /**
     * Getter.
     *
     * @return the config of storm topology.
     */
    public Config makeConfig() {

        this.stormConfig();
        this.twitterConfig();

        return conf;
    }

    /**
     * Prepare to storm related settings.
     */
    private void stormConfig() {
        if (_mode.equals("local")) {
            conf.setMaxTaskParallelism(1);
            conf.setDebug(false);
        } else if (_mode.equals("cluster")) {
            //conf.put(Config.TOPOLOGY_WORKERS, 2);
        }
    }

    /* Prepare to twitter related settings */
    private void twitterConfig() {
        String CONSUMER_KEY = "twitter.consumerKey";
        String CONSUMER_SECRET = "twitter.consumerSecret";
        String TOKEN = "twitter.token";
        String TOKEN_SECRET = "twitter.tokenSecret";
        String QUERY = "twitter.query";

        conf.put(CONSUMER_KEY, "Vkoyw2Bwgk13RFaTyJlYQ");
        conf.put(CONSUMER_SECRET, "TkW74gdR764dH6lOkD3cKSwGLMKy7xrA9s7ZCZsqRno");
        conf.put(TOKEN, "154536310-Yxg7DqA6mg982MSxG2peKa6TIUf00loFJnVMwOaP");
        conf.put(TOKEN_SECRET, "oG5JIcg1CKCDNQwqIVrt1RVR2bqPWZ91DUJXEYefnjCkX");
        conf.put(QUERY, "redborder");
    }

}
