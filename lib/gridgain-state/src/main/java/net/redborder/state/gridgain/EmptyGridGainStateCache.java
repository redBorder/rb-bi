package net.redborder.state.gridgain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by crodriguez on 19/02/15.
 */
public class EmptyGridGainStateCache<K, V> implements IGridGainStateCache<K, V> {
    @Override
    public Map<K, V> getAll(List<K> keys) {
        return new HashMap<>();
    }

    @Override
    public void putAll(Map<K, V> entries) {
        // DO NOTHING
    }
}
