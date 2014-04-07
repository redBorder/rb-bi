/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.util.scheme;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author andresgomez
 */
public class SchemeEvent_rb_monitor {

    @JsonProperty("timestamp")
    public String timestamp = null;
    @JsonProperty("sensor_id")
    public Integer sensor_id = null;
    @JsonProperty("sensor_name")
    public String sensor_name = null;
    @JsonProperty("monitor")
    public String monitor = null;
    @JsonProperty("instance")
    public String instance = null;
    @JsonProperty("value")
    public Double value = null;
    @JsonProperty("unit")
    public String unit = null;
    @JsonProperty("group_name")
    public String group_name = null;
    @JsonProperty("group_id")
    public Integer group_id = null;

}
