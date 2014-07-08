package net.redborder.state.gridgain;

import backtype.storm.task.IMetricsContext;
import backtype.storm.utils.Utils;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.NonTransactionalMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainFactory implements StateFactory {

    String _cacheName;
    List<String> _topics;

    public GridGainFactory(String cacheName, List<String> topics) {
        _cacheName = cacheName;
        _topics = topics;
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

        if (_topics.contains("darklist")) {
            GridCacheConfiguration cacheDarkList = new GridCacheConfiguration();
            cacheDarkList.setName("darklist");
            cacheDarkList.setCacheMode(GridCacheMode.PARTITIONED);
            cacheDarkList.setBackups(1);
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
