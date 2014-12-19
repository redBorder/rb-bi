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
    final String URL = "jdbc:postgresql://postgresql.redborder.cluster:5432/redborder";


    private PostgresqlManager() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static PostgresqlManager getInstance() {
        if (instance == null) {
            instance = new PostgresqlManager();
        }
        return instance;
    }

    private void initConnection() {
        try {
            conn = DriverManager.getConnection(URL, "redborder", null);
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

    public Map getAPLocation()  {

        Map<String, Map<String, Object>> map = new HashMap<>();

        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT access_points.ip_address, " +
                    "access_points.mac_address, floor.name AS floor_name, " +
                    "building.name AS building_name, campus.name AS campus_name " +
                    "FROM access_points JOIN sensors AS floor ON floor.id = access_points.sensor_id " +
                    "JOIN sensors AS building ON floor.parent_id = building.id JOIN sensors AS campus " +
                    "ON building.parent_id = campus.id;");

            while (rs.next()) {
                Map<String, Object> location = new HashMap<>();
                location.put("client_campus", rs.getString("campus_name"));
                location.put("client_building",rs.getString("building_name"));
                location.put("client_floor", rs.getString("floor_name"));
                map.put(rs.getString("mac_address"), location);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return map;
    }
}
