package net.redborder.state.gridgain;

import net.redborder.state.gridgain.util.RbLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by crodriguez on 19/02/15.
 */
public class EmptyGridGainStateCache<K, V> implements IGridGainStateCache<K, V> {
    private static Logger logger = RbLogger.getLogger(EmptyGridGainStateCache.class.getName());

    @Override
    public Map<K, V> getAll(List<K> keys) {
        logger.fine("Get request to an empty gridgain state cache");
        return new HashMap<>();
    }

    @Override
    public void putAll(Map<K, V> entries) {
        // DO NOTHING
    }
}
