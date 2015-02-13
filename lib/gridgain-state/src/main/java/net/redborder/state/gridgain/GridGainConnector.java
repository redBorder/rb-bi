package net.redborder.state.gridgain;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 13/2/15.
 */
public class GridGainConnector extends Thread{
    @Override
    public void run() {
        Logger.getLogger(GridGainConnector.class.getName()).log(Level.SEVERE, "GridGainConnector starting ...");
        GridGainManager.reconnect();
        GridGainManager.getGrid();
        Logger.getLogger(GridGainConnector.class.getName()).log(Level.SEVERE, "GridGainConnector end!!");
    }
}
