import com.hazelcast.core.HazelcastInstance;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 28/06/14.
 */
public class HazelcastState<T> implements IBackingMap<T> {
    HazelcastOptions _options;
    Map<String, T> _map;

    public HazelcastState(HazelcastOptions options, HazelcastInstance hazelcast) {
        _options = options;
        _map = hazelcast.getMap(_options.mapName);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> values = new ArrayList<T>();

        for(int i=0; i< keys.size(); i++){
            values.add(_map.get(keys.get(i).get(0).toString()));
        }

        return values;
    }


    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        for(int i=0; i< keys.size(); i++){
            _map.put(keys.get(i).get(0).toString(), values.get(i));
        }
    }
}
