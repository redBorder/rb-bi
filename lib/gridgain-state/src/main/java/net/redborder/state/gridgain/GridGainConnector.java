package net.redborder.state.gridgain;

import net.redborder.state.gridgain.util.RbLogger;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 13/2/15.
 */
public class GridGainConnector extends Thread {
    private static Logger logger = RbLogger.getLogger(GridGainConnector.class.getName());

    @Override
    public void run() {
        logger.log(Level.SEVERE, "GridGainConnector starting ...");
        GridGainManager.close();
        GridGainManager.connect();
        logger.log(Level.SEVERE, "GridGainConnector end!!");
    }
}
