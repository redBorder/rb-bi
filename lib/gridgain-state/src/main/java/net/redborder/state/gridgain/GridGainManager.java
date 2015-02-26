package net.redborder.state.gridgain;

import net.redborder.state.gridgain.util.RbLogger;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.GridCache;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 4/2/15.
 */
public class GridGainManager {
    private static final long GRIDGAIN_CONNECT_TIMEOUT = 60000l;
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

    // Thread that connects to gridgain
    private static GridGainConnector connector;

    // Time where the connect method started in order to check timeouts
    private static long startedConnectTime = 0;

    public static synchronized void init(List<String> topics, Map<String, Object> gridGainConfig) {
        logger.log(Level.FINE, "Initializing GridGainManager");

        if (!state.equals(ConnState.DISCONNECTED)) return;

        GridGainStateConfiguration.init(topics, gridGainConfig);
        gridConfig = GridGainStateConfiguration.buildConfig();

        // We initialize the empty cache that the users will share when
        // not connected to GridGain and the map that will store the caches
        // that the user will use when connected
        emptyCache = new EmptyGridGainStateCache();
        connectedCaches = new ConcurrentHashMap<>();

        // Connect asynchronously
        asyncReconnect();

        logger.log(Level.FINE, "Initialized GridGainManager");
    }

    public static IGridGainStateCache cache(String name) {
        IGridGainStateCache result = emptyCache;
        logger.severe("[State " + state.name() + "] Asking for cache " + name);

        if (state.equals(ConnState.CONNECTED)) {
            if (!connectedCaches.containsKey(name)) {
                logger.severe("[State " + state.name() + "] Im connected but cache " + name + " is not in the cache list, soy lets try to create one");
                GridCache cache = tryCache(name);

                if (cache != null) {
                    ConnectedGridGainStateCache connectedCache = new ConnectedGridGainStateCache(cache);
                    connectedCaches.put(name, connectedCache);
                    result = connectedCache;
                    logger.severe("[State " + state.name() + "] Created connected cache for cache " + name);
                }
            } else {
                logger.severe("[State " + state.name() + "] Im connected and the cache " + name + " is in the cache list so I return it");
                result = connectedCaches.get(name);
            }
        } else if (state.equals(ConnState.DISCONNECTED)) {
            logger.severe("[State " + state.name() + "] Im disconnected so I will return an empty cache for the cache " + name + " and I will try to reconnect async");
            asyncReconnect();
        }

        // If im connecting, I do nothing, therefore
        // the client will use the empty cache
        logger.severe("[State " + state.name() + "] I returned " + result + " to the cache " + name);

        return result;
    }

    private static GridCache tryCache(String name) {
        GridCache cache = null;

        if (state.equals(ConnState.CONNECTED)) {
            logger.severe("[State " + state.name() + "] Trying to get cache " + name + " from the grid cause im connected to it");

            try {
                cache = grid.cache(name);
            } catch (RuntimeException e) {
                logger.log(Level.SEVERE, "Runtime exception when calling cache: " + e.getMessage());
                notifyFail();
            }
        } else {
            logger.severe("[State " + state.name() + "] Tried to get cache " + name + " from the grid, but im not connected");
        }

        return cache;
    }

    public static void notifyFail() {
        if (state.equals(ConnState.CONNECTED)) {
            logger.severe("[State " + state.name() + "] Apparently im connected, but I received a fail notify. lets try to reconnect");
            asyncReconnect();
        } else if (state.equals(ConnState.DISCONNECTED)) {
            logger.severe("[State " + state.name() + "] Im currently disconnected, lets try to reconnect");
            asyncReconnect();
        } else if (state.equals(ConnState.CONNECTING)) {
            logger.severe("[State " + state.name() + "] Im currently trying to connect, relax... ");
        } else {
            logger.severe("[State " + state.name() + "] They notified a fail but I dont even know what Im doing");
        }
    }

    public static void connect() {
        logger.severe("[State " + state.name() + "] Gridgain connect start" );

        if (state.equals(ConnState.DISCONNECTED) || state.equals(ConnState.CONNECTING)) {
            state = ConnState.CONNECTING;
            logger.severe("[State " + state.name() + "] Connecting to gridgain grid");

            try {
                logger.severe("[State " + state.name() + "] Starting gridgain");
                grid = GridGain.start(gridConfig);

                try {
                    logger.severe("[State " + state.name() + "] Waiting for gridgain to complete startup");
                    Thread.sleep(5000);
                    logger.severe("[State " + state.name() + "] Gridgain wait finished!");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                state = ConnState.CONNECTED;
                logger.severe("[State " + state.name() + "] Gridgain started. Im connected!");
            } catch (GridException e) {
                logger.log(Level.SEVERE, e.getMessage());
                state = ConnState.DISCONNECTED;
            }
        } else {
            logger.severe("[State " + state.name() + "] Connect was called but im not disconnected nor connecting!");
        }

        logger.severe("[State " + state.name() + "] Gridgain connect end");
    }

    public static void close() {
        logger.severe("[State " + state.name() + "] Gridgain close start");

        if (!GridGain.state().equals(org.gridgain.grid.GridGainState.STOPPED)) {
            logger.severe("[State " + state.name() + "] Gridgain is not stopped. Stopping it.");
            GridGain.stopAll(true);
            logger.severe("[State " + state.name() + "] Stopped all on gridgain done.");
        }

        if (grid != null) {
            try {
                logger.severe("[State " + state.name() + "] Closing gridgain");
                grid.close();
                logger.severe("[State " + state.name() + "] Closed gridgain");
            } catch (GridException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }

            grid = null;
            connectedCaches.clear();

            if (!state.equals(ConnState.CONNECTING)) {
                logger.severe("[State " + state.name() + "] Im disconnected from gridgain now");
                state = ConnState.DISCONNECTED;
            }
        }

        logger.severe("[State " + state.name() + "] Gridgain close end");
    }

    public static synchronized void asyncReconnect() {
        logger.severe("[State " + state.name() + "] Async reconnect to gridgain start.");
        long actualTime = System.currentTimeMillis();

        if (!state.equals(ConnState.CONNECTING)) {
            state = ConnState.CONNECTING;
            logger.severe("[State " + state.name() + "] Starting gridgain connector");
            startedConnectTime = actualTime;

            try {
                connector = new GridGainConnector();
                connector.start();
            } catch (RuntimeException e) {
                logger.severe("[State " + state.name() + "] Error creating thread gridgain connector: " + e.getMessage());
                state = ConnState.DISCONNECTED;
            }
        } else if (startedConnectTime + GRIDGAIN_CONNECT_TIMEOUT > actualTime) {
            logger.severe("[State " + state.name() + "] Interrumpting gridgain connector ID: " + connector.getId() + " cause timeout was reached");

            try {
                connector.interrupt();
            } catch (RuntimeException e) {
                logger.severe("[State " + state.name() + "] Error interrumpting gridgain connector: " + e.getMessage());
            }

            state = ConnState.DISCONNECTED;
        } else {
            logger.severe("[State " + state.name() + "] Didnt start gridgain connector cause im already connecting");
        }
        logger.severe("[State " + state.name() + "] Async reconnect to gridgain end");
    }
}
