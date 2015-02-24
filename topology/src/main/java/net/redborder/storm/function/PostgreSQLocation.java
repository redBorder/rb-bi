package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import net.redborder.storm.util.PostgresqlManager;
import net.redborder.storm.util.logger.RbLogger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 19/12/14.
 */
public class PostgreSQLocation extends BaseFunction {

    PostgresqlManager _manager;
    String user;
    String uri;
    String pass;
    long updateTime, failUpdateTime;
    Logger logger;

    public PostgreSQLocation(String uri, String user, String pass, long updateTime, long failUpdateTime){
        this.uri = uri;
        this.user = user;
        this.pass = pass;
        this.updateTime = updateTime;
        this.failUpdateTime = failUpdateTime;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        logger = RbLogger.getLogger(PostgreSQLocation.class.getName());
        PostgresqlManager.initConfig(uri, user, pass, updateTime, failUpdateTime);
        _manager = PostgresqlManager.getInstance();
        _manager.init();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> flow = (Map<String, Object>) tuple.getValue(0);
        String wireless_station = (String) flow.get("wireless_station");

        if (wireless_station != null) {
             collector.emit(new Values(PostgresqlManager.get(wireless_station)));
        } else {
            collector.emit(new Values(new HashMap<String, Object>()));
        }
    }

    @Override
    public void cleanup() {
        _manager.close();
    }
}
