/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ho.yaml.Yaml;

/**
 *
 * @author andresgomez
 */
public class MemcachedConfigFile {

    List<InetSocketAddress> memcachedServer = null;
    long timeout = 60 * 5 * 1000;

    public MemcachedConfigFile() throws FileNotFoundException {

        Object object = Yaml.load(new File("/opt/rb/etc/redBorder-BI/memcached_config.yml"));
        Map<String, Object> map = (Map<String, Object>) object;
        Map<String, Object> production = (Map<String, Object>) map.get("production");
        List<String> servers = null;
        memcachedServer = new ArrayList<>();

        servers = (List<String>) production.get("servers");

        if (servers != null) {
            for (String server : servers) {
                String[] iPort = server.split(":");
                memcachedServer.add(new InetSocketAddress(iPort[0], Integer.parseInt(iPort[1])));
            }
        } else {
            Logger.getLogger(MemcachedConfigFile.class.getName()).log(Level.SEVERE, "Servers not found.");
        }

        Long timeoutLong = new Long((Integer) production.get("timeout"));
        this.timeout = timeoutLong;
        
    }

    public List<InetSocketAddress> getServers() {
        if (memcachedServer == null) {
            Logger.getLogger(MemcachedConfigFile.class.getName()).log(Level.SEVERE, "First call builder() method, "
                    + "default: {localhost:11211}");
            memcachedServer = new ArrayList<>();
            memcachedServer.add(new InetSocketAddress("localhost", 11211));

        }
        return memcachedServer;
    }

    public long getTimeOut() {
        return timeout;
    }
}
