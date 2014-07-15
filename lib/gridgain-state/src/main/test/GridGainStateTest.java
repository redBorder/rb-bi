import net.redborder.state.gridgain.GridGainState;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 14/07/14.
 */
public class GridGainStateTest {

    @Test
    public void gridGainStateTest() throws GridException {
        List<String> topics = new ArrayList<String>();
        topics.add("location");

        GridConfiguration ggConf = new GridConfiguration();
        GridCacheConfiguration cacheLocation = new GridCacheConfiguration();
        cacheLocation.setName("location");
        cacheLocation.setCacheMode(GridCacheMode.PARTITIONED);
        ggConf.setCacheConfiguration(cacheLocation);
        Grid grid = GridGain.start(ggConf);

        GridCache<String, Map<String, Object>> map = grid.cache("location");

        GridGainState state = new GridGainState(map);

        List<List<Object>> keysList = new ArrayList<>();

        List<Object> keys = new ArrayList<>();
        keys.add("A");
        keysList.add(keys);

        keys = new ArrayList<>();
        keys.add("B");
        keysList.add(keys);

        keys = new ArrayList<>();
        keys.add("C");
        keysList.add(keys);

        List<Map<String, Map<String, Object>>> valuesList = new ArrayList<>();
        Map<String, Map<String, Object>> values = new HashMap<>();

        Map<String, Object> value = new HashMap<>();

        value.put("A1", 1);
        values.put("A", value);

        value = new HashMap<>();
        value.put("B1", 2);
        values.put("B", value);

        value = new HashMap<>();
        value.put("C1", 3);
        values.put("C", value);

        valuesList.add(values);


        state.multiPut(keysList, valuesList);


        List<Map<String, Map<String, Object>>> restoreList = state.multiGet(keysList);
        Map<String, Map<String, Object>> restore = restoreList.get(0);

        Assert.assertEquals(values.get("A"), restore.get("A"));
        Assert.assertEquals(values.get("A").get("A1"), restore.get("A").get("A1"));
        Assert.assertEquals(values.get("B"), restore.get("B"));
        Assert.assertEquals(values.get("B").get("B1"), restore.get("B").get("B1"));
        Assert.assertEquals(values.get("C"), restore.get("C"));
        Assert.assertEquals(values.get("C").get("C1"), restore.get("C").get("C1"));
        Assert.assertNotEquals(values.get("C").get("C1"), restore.get("B").get("B1"));

        grid.close();
    }
}
