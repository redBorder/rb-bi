package net.redborder.state.gridgain;

import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;

/**
 * Created by andresgomez on 01/07/14.
 */
public class GridGainClient{



    public static void create(){

        try {

            Grid grid = GridGain.start("/opt/rb/etc/redBorder-BI/gridgain.xml");

        } catch (GridException e) {
            e.printStackTrace();
        }


    }

}
