package net.redborder.state.gridgain;

import com.amazonaws.auth.BasicAWSCredentials;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.s3.GridTcpDiscoveryS3IpFinder;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by crodriguez on 19/02/15.
 */
public class GridGainStateConfiguration {
    public static final Long TIME_TO_LIVE = 60 * 60 * 1000L;
    private final static String CONFIG_FILE_PATH = "/opt/rb/etc/darklist_config.yml";

    private static GridCacheDistributionMode _mode = GridCacheDistributionMode.CLIENT_ONLY;
    private static Long _timeToLive;
    private static List<String> _gridGainServers;
    private static Map<String, Object> _s3Config;
    private static List<String> _topics;
    private static Long _timeout;

    public static void init(List<String> topics, Map<String, Object> gridGainConfig) {
        _topics = topics;
        _timeToLive = gridGainConfig.get("time_to_live") == null ? TIME_TO_LIVE : (Long.valueOf(gridGainConfig.get("time_to_live").toString()));

        String cacheType = (String) gridGainConfig.get("cache_type");
        if (cacheType != null && cacheType.equals("partitioned")) {
            _mode = GridCacheDistributionMode.PARTITIONED_ONLY;
        }

        if (!gridGainConfig.containsKey("s3")) {
            _gridGainServers = (List<String>) gridGainConfig.get("servers");
            Integer timeout = (Integer) gridGainConfig.get("timeout");
            _timeout = (timeout != null) ? Long.valueOf(timeout) : 5000l;
        } else {
            _s3Config = (Map<String, Object>) gridGainConfig.get("s3");
        }
    }

    public static GridConfiguration buildConfig() {
        GridConfiguration conf = new GridConfiguration();
        List<GridCacheConfiguration> caches = new ArrayList<GridCacheConfiguration>();
        GridTcpDiscoverySpi gridTcp = new GridTcpDiscoverySpi();


        if (_s3Config == null) {
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

        if (_timeout != null)
            gridTcp.setNetworkTimeout(_timeout);

        conf.setDiscoverySpi(gridTcp);

        System.out.println("TOPICS: " + _topics);
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
            cacheDarkList.setDistributionMode(_mode);
            caches.add(cacheDarkList);
        }

        if (_topics.contains("mobile")) {
            GridCacheConfiguration cacheMobile = new GridCacheConfiguration();
            cacheMobile.setName("mobile");
            cacheMobile.setDistributionMode(_mode);
            cacheMobile.setDefaultTimeToLive(_timeToLive);
            cacheMobile.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheMobile);
        }

        if (_topics.contains("radius")) {
            GridCacheConfiguration cacheRadius = new GridCacheConfiguration();
            cacheRadius.setName("radius");
            cacheRadius.setDistributionMode(_mode);
            cacheRadius.setDefaultTimeToLive(_timeToLive);
            cacheRadius.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheRadius);
        }

        if (_topics.contains("location")) {
            GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
            cacheLocation.setName("location");
            cacheLocation.setDistributionMode(_mode);
            cacheLocation.setDefaultTimeToLive(_timeToLive);
            cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheLocation);

            GridCacheConfiguration cacheLocationInfp = new GridCacheConfiguration();
            cacheLocationInfp.setName("location-info");
            cacheLocationInfp.setDistributionMode(_mode);
            cacheLocationInfp.setDefaultTimeToLive(_timeToLive);
            cacheLocationInfp.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheLocationInfp);
        }

        if (_topics.contains("nmsp")) {
            GridCacheConfiguration cacheNmsp = new GridCacheConfiguration();
            cacheNmsp.setName("nmsp");
            cacheNmsp.setDistributionMode(_mode);
            cacheNmsp.setDefaultTimeToLive(_timeToLive);
            cacheNmsp.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheNmsp);

            GridCacheConfiguration cacheNmspInfo = new GridCacheConfiguration();
            cacheNmspInfo.setName("nmsp-info");
            cacheNmspInfo.setDistributionMode(_mode);
            cacheNmspInfo.setDefaultTimeToLive(_timeToLive);
            cacheNmspInfo.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheNmspInfo);

            GridCacheConfiguration cacheNmspLocationState = new GridCacheConfiguration();
            cacheNmspLocationState.setName("nmsp-location-state");
            cacheNmspLocationState.setDistributionMode(_mode);
            cacheNmspLocationState.setDefaultTimeToLive(_timeToLive);
            cacheNmspLocationState.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheNmspLocationState);
        }

        if (_topics.contains("trap")) {
            GridCacheConfiguration cacheTrap = new GridCacheConfiguration();
            cacheTrap.setName("trap");
            cacheTrap.setDistributionMode(_mode);
            cacheTrap.setDefaultTimeToLive(_timeToLive);
            cacheTrap.setCacheMode(GridCacheMode.PARTITIONED);
            caches.add(cacheTrap);
        }

        conf.setCacheConfiguration(caches.toArray(new GridCacheConfiguration[caches.size()]));

        return conf;
    }
}
