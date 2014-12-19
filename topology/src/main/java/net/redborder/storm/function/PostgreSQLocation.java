package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import net.redborder.storm.util.PostgresqlManager;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 19/12/14.
 */
public class PostgreSQLocation extends BaseFunction {

    PostgresqlManager _manager;
    Map<String, Map<String, Object>> _hash;
    long _last_update;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        try {
            _manager = PostgresqlManager.getInstance();
            _manager.init();
            _hash = _manager.getAPLocation();
            System.out.println("AP LOCATION: " + _hash.toString());
            _last_update = System.currentTimeMillis();
        } catch (Exception ex) {
            Logger.getLogger(PostgreSQLocation.class.getName()).log(Level.WARNING,
                    "The postgreSQL query fail ... next try on 30 minutes!");
            ex.printStackTrace();
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            if (_last_update + 1800000 < System.currentTimeMillis()) {
                _hash = _manager.getAPLocation();
                System.out.println("AP LOCATION: " + _hash.toString());
                _last_update = System.currentTimeMillis();

            }

            Map<String, Object> flow = (Map<String, Object>) tuple.getValue(0);
            String wireless_station = (String) flow.get("wireless_station");

            if (wireless_station != null)
                collector.emit(new Values(_hash.get(wireless_station)));
            else
                collector.emit(new Values(new HashMap<String, Object>()));
        } catch (Exception ex) {
            _last_update = System.currentTimeMillis();
            Logger.getLogger(PostgreSQLocation.class.getName()).log(Level.WARNING,
                    "The postgreSQL query fail ... next try on 30 minutes!", ex.toString());
            ex.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        _manager.close();
    }
}
