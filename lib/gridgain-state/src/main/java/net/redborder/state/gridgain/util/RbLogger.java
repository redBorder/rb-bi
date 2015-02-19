package net.redborder.state.gridgain.util;

import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 28/1/15.
 */
public class RbLogger {

    public static Logger getLogger(String loggerName) {
        Logger logger = Logger.getLogger(loggerName);
        logger.setUseParentHandlers(false);

        RbFormatter formatter = new RbFormatter();
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(formatter);

        logger.addHandler(handler);

        return logger;
    }
}