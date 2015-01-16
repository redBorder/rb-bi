package net.redborder.storm.util;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 19/12/14.
 */
public class PostgresqlManager {

    private static Connection conn = null;
    private static PostgresqlManager instance = null;
    private static String _user;
    private static String _uri;
    private static String _pass;

    private PostgresqlManager() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void initConfig(String uri, String user, String pass) {
        _uri=uri;
        _user=user;
        _pass=pass;
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

    public void init() {
        if (conn == null)
            initConnection();
    }

    private void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        closeConnection();
    }

    public Map getAPLocation() {

        Map<String, Map<String, Object>> map = new HashMap<>();

        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT access_points.ip_address, access_points.mac_address," +
                    " access_points.latitude AS latitude, access_points.longitude AS longitude, floor.name AS floor_name," +
                    " building.name AS building_name, campus.name AS campus_name " +
                    "FROM access_points JOIN sensors AS floor ON floor.id = access_points.sensor_id " +
                    "JOIN sensors AS building ON floor.parent_id = building.id JOIN sensors " +
                    "AS campus ON building.parent_id = campus.id;");

            while (rs.next()) {

                Double longitude = (double) Math.round(Double.valueOf(rs.getString("longitude")) * 100000) / 100000;
                Double latitude = (double) Math.round(Double.valueOf(rs.getString("latitude")) * 100000) / 100000;

                Map<String, Object> location = new HashMap<>();
                location.put("client_latlong", latitude+","+longitude);
                location.put("client_campus", rs.getString("campus_name"));
                location.put("client_building", rs.getString("building_name"));
                location.put("client_floor", rs.getString("floor_name"));
                map.put(rs.getString("mac_address"), location);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return map;
    }
}
