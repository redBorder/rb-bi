package net.redborder.state.gridgain;

import net.redborder.state.gridgain.util.RbLogger;
import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by crodriguez on 19/02/15.
 */
public class ConnectedGridGainStateCache<K, V> implements IGridGainStateCache<K, V> {
    private static Logger logger = RbLogger.getLogger(ConnectedGridGainStateCache.class.getName());
    GridCache<K, V> _gridMap = null;

    public ConnectedGridGainStateCache(GridCache<K, V> gridMap) {
        setMap(gridMap);
    }

    public void setMap(GridCache<K, V> gridMap) {
        _gridMap = gridMap;
    }

    @Override
    public Map<K, V> getAll(List<K> keys) {
        Map<K, V> result;

        if (_gridMap == null) {
            result = new HashMap<>();
        } else {
            try {
                result = _gridMap.getAll(keys);
            } catch (GridException | RuntimeException e) {
                logger.log(Level.SEVERE, e.getMessage());
                GridGainManager.notifyFail();
                result = new HashMap<>();
            }
        }

        return result;
    }

    @Override
    public void putAll(Map<K, V> entries) {
        if (_gridMap == null) return;

        try {
            _gridMap.putAll(entries);
        } catch (GridException | RuntimeException e) {
            logger.log(Level.SEVERE, e.getMessage());
            GridGainManager.notifyFail();
        }
    }
}
