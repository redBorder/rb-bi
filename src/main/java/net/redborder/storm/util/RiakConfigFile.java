package net.redborder.storm.util;

import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 16/06/14.
 */
public class RiakConfigFile {

    List<String> riakServers = null;

    public RiakConfigFile(boolean debug) {
        try {
            Object object = Yaml.load(new File("/opt/rb/etc/redBorder-BI/riak_config.yml"));
            Map<String, Object> map = (Map<String, Object>) object;
            Map<String, Object> production = (Map<String, Object>) map.get("production");
            List<String> servers = null;
            riakServers = new ArrayList<>();

            servers = (List<String>) production.get("servers");

            if (servers != null) {
                for (String server : servers) {
                    riakServers.add(server);
                }

                if (debug) {
                    System.out.println(RiakConfigFile.class.getName() + " - Riak Servers: " + riakServers.toString());
                }

            } else {
                Logger.getLogger(RiakConfigFile.class.getName()).log(Level.SEVERE, "Servers not found.");
            }
        } catch (FileNotFoundException e) {
            Logger.getLogger(RiakConfigFile.class.getName()).log(Level.SEVERE, "Riak file not found.");
        }
    }

    public List<String> getServers() {
        if (riakServers == null) {
            Logger.getLogger(MemcachedConfigFile.class.getName()).log(Level.SEVERE, "First call builder() method, "
                    + "default: {localhost}");
            riakServers = new ArrayList<>();
            riakServers.add("localhost");

        }
        return riakServers;
    }
}
