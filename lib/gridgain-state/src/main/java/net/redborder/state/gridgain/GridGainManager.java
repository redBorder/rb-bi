package net.redborder.state.gridgain;

import net.redborder.state.gridgain.util.RbLogger;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.GridCache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 4/2/15.
 */
public class GridGainManager {
    private static Logger logger = RbLogger.getLogger(GridGainManager.class.getName());

    // Grid instances that we are currently using
    private static Grid grid = null;
    private static GridConfiguration gridConfig = null;

    // Caches that the clients will use
    private static EmptyGridGainStateCache emptyCache;
    private static Map<String, ConnectedGridGainStateCache> connectedCaches;

    // State of the gridgain connection
    private enum ConnState { CONNECTED, CONNECTING, DISCONNECTED }
    private static ConnState state = ConnState.DISCONNECTED;

    public static synchronized void init(List<String> topics, Map<String, Object> gridGainConfig) {
        if (!state.equals(ConnState.DISCONNECTED)) return;

        GridGainStateConfiguration.init(topics, gridGainConfig);
        gridConfig = GridGainStateConfiguration.buildConfig();

        // We initialize the empty cache that the users will share when
        // not connected to GridGain and the map that will store the caches
        // that the user will use when connected
        emptyCache = new EmptyGridGainStateCache();
        connectedCaches = new HashMap<>();

        // Connect asynchronously
        asyncReconnect();
    }

    public static IGridGainStateCache cache(String name) {
        IGridGainStateCache result = emptyCache;

        if (state.equals(ConnState.CONNECTED)) {
            if (!connectedCaches.containsKey(name)) {
                GridCache cache = tryCache(name);

                if (cache != null) {
                    ConnectedGridGainStateCache connectedCache = new ConnectedGridGainStateCache(cache);
                    connectedCaches.put(name, connectedCache);
                    result = connectedCache;
                }
            } else {
                result = connectedCaches.get(name);
            }
        } else if (state.equals(ConnState.DISCONNECTED)) {
            asyncReconnect();
        }

        // If im connecting, I do nothing, therefore
        // the client will use the empty cache

        return result;
    }

    private static GridCache tryCache(String name) {
        GridCache cache = null;

        if (state.equals(ConnState.CONNECTED)) {
            try {
                cache = grid.cache(name);
            } catch (RuntimeException e) {
                logger.log(Level.SEVERE, "Runtime exception when calling cache: " + e.getMessage());
                notifyFail();
            }
        }

        return cache;
    }

    public static synchronized void notifyFail() {
        if (state.equals(ConnState.CONNECTED)) {
            logger.log(Level.SEVERE, "Apparently im connected, but I received a fail notify. lets try to reconnect");
            asyncReconnect();
        } else if (state.equals(ConnState.DISCONNECTED)) {
            logger.log(Level.SEVERE, "Im currently disconnected, lets try to reconnect");
            asyncReconnect();
        } else if (state.equals(ConnState.CONNECTING)) {
            logger.log(Level.SEVERE, "Im currently trying to connect, relax... ");
        }
    }

    public static synchronized void connect() {
        if (grid == null) {
            state = ConnState.CONNECTING;

            try {
                grid = GridGain.start(gridConfig);
                state = ConnState.CONNECTED;
            } catch (GridException e) {
                logger.log(Level.SEVERE, e.getMessage());
                state = ConnState.DISCONNECTED;
            }
        }
    }

    public static synchronized void close() {
        if (grid != null) {
            try {
                grid.close();
            } catch (GridException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }

            grid = null;
            connectedCaches.clear();

            if (!state.equals(ConnState.CONNECTING)) {
                state = ConnState.DISCONNECTED;
            }
        }
    }

    public static synchronized void asyncReconnect() {
        if (!state.equals(ConnState.CONNECTING)) {
            state = ConnState.CONNECTING;
            GridGainConnector connector = new GridGainConnector();
            connector.start();
        }
    }
}
