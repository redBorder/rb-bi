package net.redborder.state.gridgain;

import backtype.storm.task.IMetricsContext;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.s3.GridTcpDiscoveryS3IpFinder;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder;
import org.ho.yaml.Yaml;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.NonTransactionalMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainFactory implements StateFactory {

    String _cacheName;
    List<String> _topics;
    List<String> _gridGainServers;
    String _multicastGroup;
    Map<String, Object> _s3Config = null;

    private final static String CONFIG_FILE_PATH = "/opt/rb/etc/darklist_config.yml";


    public GridGainFactory(String cacheName, List<String> topics, Map<String, Object> gridGainConfig) {
        _cacheName = cacheName;
        _topics = topics;
        if(!gridGainConfig.containsKey("s3")) {
            _gridGainServers = (List<String>) gridGainConfig.get("servers");
            _multicastGroup = (String) gridGainConfig.get("multicast");
        }
        else{
            _s3Config = (Map<String, Object>) gridGainConfig.get("s3");
        }
    }


    @Override
    public State makeState(Map configStorm, IMetricsContext iMetricsContext, int i, int i2) {
        Grid grid = null;

        try {
            grid = GridGain.start(makeConfig());
        } catch (GridException e) {
            grid = GridGain.grid();
        }

        GridCache<String, Map<String, Object>> map = grid.cache(_cacheName);
        return NonTransactionalMap.build(new GridGainState(map));
    }


    public GridConfiguration makeConfig() {
        GridConfiguration conf = new GridConfiguration();
        List<GridCacheConfiguration> caches = new ArrayList<GridCacheConfiguration>();
        GridTcpDiscoverySpi gridTcp = new GridTcpDiscoverySpi();


        if(_s3Config==null) {
            GridTcpDiscoveryVmIpFinder gridIpFinder = new GridTcpDiscoveryVmIpFinder();

            Collection<InetSocketAddress> ips = new ArrayList<>();

            try {
                conf.setLocalHost(InetAddress.getLocalHost().getHostName());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

            if (_gridGainServers != null) {
                for (String server : _gridGainServers) {
                    String[] serverPort = server.split(":");
                    ips.add(new InetSocketAddress(serverPort[0], Integer.valueOf(serverPort[1])));
                }

                gridIpFinder.registerAddresses(ips);
            }

            gridTcp.setIpFinder(gridIpFinder);

        } else {
            GridTcpDiscoveryS3IpFinder s3IpFinder = new GridTcpDiscoveryS3IpFinder();
            s3IpFinder.setBucketName(_s3Config.get("bucket").toString());
            s3IpFinder.setAwsCredentials(new BasicAWSCredentials(_s3Config.get("access_key").toString(), _s3Config.get("secret_key").toString()));
            gridTcp.setIpFinder(s3IpFinder);
        }

        conf.setDiscoverySpi(gridTcp);

        if (_topics.contains("darklist")) {

            Map<String, Object> general = null;
            Integer backups = 0;

            try {
                Map<String, Object> configMap = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));
                general = (Map<String, Object>) configMap.get("general");
                backups = (Integer) general.get("backups");
                if (backups == null) {
                    backups = 0;
                }
            } catch (FileNotFoundException e) {
                backups = 0;
                e.printStackTrace();
            }


            GridCacheConfiguration cacheDarkList = new GridCacheConfiguration();
            cacheDarkList.setName("darklist");
            cacheDarkList.setCacheMode(GridCacheMode.PARTITIONED);
            cacheDarkList.setBackups(backups);
            cacheDarkList.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
            cacheDarkList.setStartSize(2 * 1024 * 1024);
            cacheDarkList.setOffHeapMaxMemory(0);
            cacheDarkList.setPreloadBatchSize(1024 * 1024);
            cacheDarkList.setPreloadThreadPoolSize(4);
            caches.add(cacheDarkList);
        }

        if (_topics.contains("mobile")) {
            GridCacheConfiguration cacheMobile = new GridCacheConfiguration();
            cacheMobile.setName("mobile");
            cacheMobile.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
            cacheMobile.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheMobile);
        }

        if (_topics.contains("radius")) {
            GridCacheConfiguration cacheRadius = new GridCacheConfiguration();
            cacheRadius.setName("radius");
            cacheRadius.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
            cacheRadius.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheRadius);
        }

        if (_topics.contains("location")) {
            GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
            cacheLocation.setName("location");
            cacheLocation.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
            cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheLocation);
        }

        if (_topics.contains("trap")) {
            GridCacheConfiguration cacheTrap = new GridCacheConfiguration();
            cacheTrap.setName("trap");
            cacheTrap.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
            cacheTrap.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheTrap);
        }

        conf.setCacheConfiguration(caches.toArray(new GridCacheConfiguration[caches.size()]));

        return conf;
    }
}
