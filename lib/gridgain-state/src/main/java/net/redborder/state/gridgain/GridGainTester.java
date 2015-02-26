package net.redborder.state.gridgain;

import net.redborder.state.gridgain.util.RbLogger;

import java.util.logging.Logger;

/**
 * Created by andresgomez on 13/2/15.
 */
public class GridGainTester extends Thread {
    private static Logger logger = RbLogger.getLogger(GridGainTester.class.getName());

    @Override
    public void run() {
        logger.severe("GridGainConnector starting");
        GridGainManager.close();
        GridGainManager.connect();
        logger.severe("GridGainConnector end");
    }
}
