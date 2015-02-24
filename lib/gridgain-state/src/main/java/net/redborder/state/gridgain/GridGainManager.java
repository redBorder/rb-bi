package net.redborder.state.gridgain;

import net.redborder.state.gridgain.util.RbLogger;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
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
        logger.log(Level.FINE, "Initializing GridGainManager");

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

        logger.log(Level.FINE, "Initialized GridGainManager");
    }

    public static IGridGainStateCache cache(String name) {
        IGridGainStateCache result = emptyCache;
        logger.severe("Asking for cache " + name);

        if (state.equals(ConnState.CONNECTED)) {
            if (!connectedCaches.containsKey(name)) {
                logger.severe("Im connected but cache " + name + " is not in the cache list, soy lets try to create one");
                GridCache cache = tryCache(name);

                if (cache != null) {
                    ConnectedGridGainStateCache connectedCache = new ConnectedGridGainStateCache(cache);
                    connectedCaches.put(name, connectedCache);
                    result = connectedCache;
                    logger.severe("Created connected cache for cache " + name);
                }
            } else {
                logger.severe("Im connected and the cache " + name + " is in the cache list so I return it");
                result = connectedCaches.get(name);
            }
        } else if (state.equals(ConnState.DISCONNECTED)) {
            logger.severe("Im disconnected so I will return an empty cache for the cache " + name + " and I will try to reconnect async");
            asyncReconnect();
        }

        // If im connecting, I do nothing, therefore
        // the client will use the empty cache
        logger.severe("I returned " + result + " to the cache " + name);

        return result;
    }

    private static GridCache tryCache(String name) {
        GridCache cache = null;

        if (state.equals(ConnState.CONNECTED)) {
            logger.severe("Trying to get cache " + name + " from the grid cause im connected to it");

            try {
                cache = grid.cache(name);
            } catch (RuntimeException e) {
                logger.log(Level.SEVERE, "Runtime exception when calling cache: " + e.getMessage());
                notifyFail();
            }
        } else {
            logger.severe("Tried to get cache " + name + " from the grid, but im not connected -> " + state);
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
        } else {
            logger.log(Level.SEVERE, "They notified a fail but I dont even know what Im doing");
        }
    }

    public static synchronized void connect() {
        logger.severe("Gridgain connect start");

        if (grid == null) {
            state = ConnState.CONNECTING;
            logger.severe("Connecting to gridgain grid");

            try {
                logger.severe("Starting gridgain");
                grid = GridGain.start(gridConfig);
                state = ConnState.CONNECTED;
                logger.severe("Gridgain started. Im connected!");
            } catch (GridException e) {
                logger.log(Level.SEVERE, e.getMessage());
                state = ConnState.DISCONNECTED;
            }
        }

        logger.severe("Gridgain connect end");
    }

    public static synchronized void close() {
        logger.severe("Gridgain close start");

        if (grid != null) {
            try {
                logger.severe("Closing gridgain");
                grid.close();
                logger.severe("Closed gridgain");
            } catch (GridException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }

            grid = null;
            connectedCaches.clear();

            if (!state.equals(ConnState.CONNECTING)) {
                logger.severe("Im disconnected from gridgain now");
                state = ConnState.DISCONNECTED;
            }
        }

        logger.severe("Gridgain close end");
    }

    public static synchronized void asyncReconnect() {
        logger.severe("Async reconnect to gridgain start");
        if (!state.equals(ConnState.CONNECTING)) {
            state = ConnState.CONNECTING;
            GridGainConnector connector = new GridGainConnector();
            logger.severe("Starting gridgain connector");
            connector.start();
        }
        logger.severe("Async reconnect to gridgain end");
    }
}
