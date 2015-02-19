package net.redborder.state.gridgain;

import java.util.List;
import java.util.Map;

/**
 * Created by crodriguez on 19/02/15.
 */
public interface IGridGainStateCache<K, V> {
    public Map<K, V> getAll(List<K> keys);
    public void putAll(Map<K, V> entries);
}
