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

    GridGainOptions _options;

    public GridGainFactory(GridGainOptions options) {
        _options = options;

    }

    @Override
    public State makeState(Map configStorm, IMetricsContext iMetricsContext, int i, int i2) {

        GridCacheConfiguration cacheConf = new GridCacheConfiguration();
        cacheConf.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);


        GridConfiguration conf = new GridConfiguration();

        switch (_options.cacheMode) {
            case 0:
                cacheConf.setCacheMode(GridCacheMode.LOCAL);
                break;
            case 1:
                cacheConf.setCacheMode(GridCacheMode.REPLICATED);
                break;
            case 2:
                cacheConf.setCacheMode(GridCacheMode.PARTITIONED);
                break;
        }

        cacheConf.setName(_options.cacheName);
        cacheConf.setBackups(_options.backups);

        conf.setCacheConfiguration(cacheConf);

        GridCache<K, V> map = null;

        try {

            Grid grid = GridGain.start(conf);
            map = grid.cache(_options.cacheName);

        } catch (GridException e) {
            e.printStackTrace();
        }

        return NonTransactionalMap.build(new GridGainState<K, V>(map));
    }
}
