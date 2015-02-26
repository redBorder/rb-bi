package net.redborder.storm.util;

import net.redborder.storm.util.logger.RbLogger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 19/12/14.
 */
public class PostgresqlManager {
    private static Connection conn = null;
    private static PostgresqlManager instance = null;
    private static String _user;
    private static String _uri;
    private static String _pass;
    private static Logger logger = RbLogger.getLogger(PostgresqlManager.class.getName());
    private static Map<String, Map<String, Object>> _hash;
    private static  long _last_update, _next_update, _update_time, _fail_update_time;

    private PostgresqlManager() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void initConfig(String uri, String user, String pass, long updateTime, long failUpdateTime) {
        _uri = uri;
        _user = user;
        _pass = pass;
        _update_time = updateTime;
        _fail_update_time = failUpdateTime;
    }

    public static PostgresqlManager getInstance() {
        if (_uri != null && _user != null) {
            if (instance == null) {
                instance = new PostgresqlManager();
            }
        } else {
            System.out.println("You must call initConfig first!");
        }
        return instance;
    }

    private void initConnection() {
        try {
            if (_uri != null && _user != null)
                conn = DriverManager.getConnection(_uri, _user, _pass);
            else
                System.out.println("You must initialize the db_uri and db_user at bi_config file.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public synchronized void init() {
        if (conn == null) {
            initConnection();
            _hash = new HashMap<>();
        }
    }

    private synchronized void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void close() {
        closeConnection();
    }

    public static Map<String, Object> get(String apName) {
        if ((_last_update + _next_update) < System.currentTimeMillis()) {
            // logger.severe("Updating PostgreSQL data");
            updateAPLocation();
            // logger.severe("Finished postgresql update. Last update: " + _last_update + " Next update: " + _next_update);
        }

        return _hash.get(apName);
    }

    public synchronized static void updateAPLocation() {
        Map<String, Map<String, Object>> map = new HashMap<>();
        Statement st = null;
        ResultSet rs = null;

        try {
            // logger.severe("Started postgresql query...");
            st = conn.createStatement();
            rs = st.executeQuery("SELECT access_points.ip_address, access_points.mac_address, access_points.latitude AS latitude, access_points.longitude AS longitude, floor.name AS floor_name,building.name AS building_name, campus.name AS campus_name, zones.name AS zone_name FROM access_points FULL OUTER JOIN sensors AS floor ON floor.id = access_points.sensor_id FULL OUTER JOIN sensors AS building ON floor.parent_id = building.id FULL OUTER JOIN sensors AS campus ON building.parent_id = campus.id FULL OUTER JOIN (SELECT MIN(zone_id) AS zone_id, access_point_id FROM access_points_zones GROUP BY access_point_id) AS zones_ids ON access_points.id = zones_ids.access_point_id FULL OUTER JOIN zones ON zones_ids.zone_id = zones.id WHERE access_points.mac_address IS NOT NULL;");
            // logger.severe("Finished postgresql query! Parsing information...");

            while (rs.next()) {
                Map<String, Object> location = new HashMap<>();

                if (rs.getString("longitude") != null && rs.getString("latitude") != null) {
                    Double longitude = (double) Math.round(Double.valueOf(rs.getString("longitude")) * 100000) / 100000;
                    Double latitude = (double) Math.round(Double.valueOf(rs.getString("latitude")) * 100000) / 100000;
                    location.put("client_latlong", latitude + "," + longitude);
                }

                if (rs.getString("campus_name") != null)
                    location.put("client_campus", rs.getString("campus_name"));
                else
                    location.put("client_campus", "unknown");

                if (rs.getString("building_name") != null)
                    location.put("client_building", rs.getString("building_name"));
                else
                    location.put("client_building", "unknown");

                if (rs.getString("floor_name") != null)
                    location.put("client_floor", rs.getString("floor_name"));
                else
                    location.put("client_floor", "unknown");

                if (rs.getString("zone_name") != null)
                    location.put("client_zone", rs.getString("zone_name"));
                else
                    location.put("client_zone", "unknown");


                if (!location.isEmpty())
                    map.put(rs.getString("mac_address"), location);
            }

            _last_update = System.currentTimeMillis();
            _next_update = _update_time;
            logger.info("Location Entry: " + _hash.size() + " \n   Updated data: \n " + _hash.toString());
            // logger.severe("Finished parsing postgresql query!");
        } catch (SQLException e) {
            _last_update = System.currentTimeMillis();
            _next_update = _fail_update_time;
            logger.severe("The postgreSQL query failed! " + e.toString());
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception e) {}
            try { if (st != null) st.close(); } catch (Exception e) {}
        }

        _hash = map;
    }
}
