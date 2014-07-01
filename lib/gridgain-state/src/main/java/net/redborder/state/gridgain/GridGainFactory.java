package net.redborder.state.gridgain;

import backtype.storm.task.IMetricsContext;
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

import java.util.Map;

/**
 * Created by andresgomez on 30/06/14.
 */
public class GridGainFactory<K, V> implements StateFactory {

    String _cacheName;

    public GridGainFactory(String cacheName) {
        _cacheName = cacheName;
    }


    @Override
    public State makeState(Map configStorm, IMetricsContext iMetricsContext, int i, int i2) {

        Grid grid = null;



        if (GridGain.state("storm").equals(org.gridgain.grid.GridGainState.STARTED)) {
            grid = GridGain.grid("storm");
        } else {
            try {
                grid = GridGain.start(makeConfig());
            } catch (GridException e) {
                e.printStackTrace();
            }
        }

        GridCache<K, V> map = grid.cache(_cacheName);
        return NonTransactionalMap.build(new GridGainState<K, V>(map));
    }


    public GridConfiguration makeConfig(){
        GridConfiguration conf = new GridConfiguration();

        conf.setGridName("storm");

        GridCacheConfiguration cacheDarkList = new GridCacheConfiguration();
        cacheDarkList.setName("darklist");
        cacheDarkList.setCacheMode(GridCacheMode.PARTITIONED);
        cacheDarkList.setBackups(1);
        cacheDarkList.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
        cacheDarkList.setStartSize(2 * 1024 * 1024);
        cacheDarkList.setOffHeapMaxMemory(0);
        cacheDarkList.setPreloadBatchSize(1024 * 1024);
        cacheDarkList.setPreloadThreadPoolSize(4);

        GridCacheConfiguration cacheMobile = new GridCacheConfiguration();
        cacheMobile.setName("mobile");
        cacheMobile.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
        cacheMobile.setCacheMode(GridCacheMode.PARTITIONED);

        GridCacheConfiguration cacheRadius = new GridCacheConfiguration();
        cacheMobile.setName("radius");
        cacheMobile.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
        cacheMobile.setCacheMode(GridCacheMode.PARTITIONED);

        GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
        cacheLocation.setName("location");
        cacheLocation.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
        cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);

        GridCacheConfiguration cacheTrap = new GridCacheConfiguration();
        cacheTrap.setName("trap");
        cacheTrap.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);
        cacheTrap.setCacheMode(GridCacheMode.PARTITIONED);

        conf.setCacheConfiguration(cacheDarkList, cacheMobile, cacheRadius
                , cacheLocation, cacheTrap);

        return conf;
    }
}
