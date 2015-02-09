package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import net.redborder.storm.util.ConfigData;
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
    String user;
    String uri;
    String pass;
    long _next_update;

    public PostgreSQLocation(String uri, String user, String pass){
        this.uri=uri;
        this.user=user;
        this.pass=pass;
        this._next_update=1800000;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        try {
            PostgresqlManager.initConfig(uri, user, pass);
            _manager = PostgresqlManager.getInstance();
            _manager.init();
            _hash = _manager.getAPLocation();
            _last_update = System.currentTimeMillis();
            _next_update = 1800000;
            Logger.getLogger(PostgreSQLocation.class.getName()).log(Level.INFO,
                    "Initiate location with postgreSQL info. \n Location Entry: " + _hash.size()
                            + " \n   Initial data: \n " + _hash.toString());
        } catch (Exception ex) {
            _next_update = 300000;
            Logger.getLogger(PostgreSQLocation.class.getName()).log(Level.WARNING,
                    "The postgreSQL query fail ... next try on 5 minutes!");
            ex.printStackTrace();
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            if ((_last_update + _next_update) < System.currentTimeMillis()) {
                _hash = _manager.getAPLocation();
                Logger.getLogger(PostgreSQLocation.class.getName()).log(Level.INFO,
                        "Update location with postgreSQL info. \n Location Entry: " + _hash.size()
                + " \n   Updated data: \n " + _hash.toString());
                _last_update = System.currentTimeMillis();
                _next_update = 1800000;
            }

            Map<String, Object> flow = (Map<String, Object>) tuple.getValue(0);
            String wireless_station = (String) flow.get("wireless_station");

            if (wireless_station != null)
                collector.emit(new Values(_hash.get(wireless_station)));
            else
                collector.emit(new Values(new HashMap<String, Object>()));
        } catch (Exception ex) {
            _last_update = System.currentTimeMillis();
            _next_update = 300000;
            Logger.getLogger(PostgreSQLocation.class.getName()).log(Level.WARNING,
                    "The postgreSQL query fail ... next try on 5 minutes!", ex.toString());
            ex.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        _manager.close();
    }
}
