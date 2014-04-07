/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip.timeZone;
import net.redborder.storm.util.CheckIp;
import net.redborder.storm.util.RBEventType;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;

/**
 *
 * @author andresgomez
 */
public class GeoIpBolt extends BaseRichBolt {

    OutputCollector _collector;

    String _citydb;
    String _countrydb;
    String _asndb;
    String _countryv6db;
    String _asnv6db;
    String _cityv6db;

    Map<String, Object> event;

    boolean geoIPevent;

    LookupService _city;
    LookupService _city6;
    LookupService _asn;
    LookupService _asn6;

    int i;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("geoIpEvent", "topic"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        _citydb = "/opt/rb/share/GeoIP/city.dat";
        _asndb = "/opt/rb/share/GeoIP/asn.dat";
        _asnv6db = "/opt/rb/share/GeoIP/asnv6.dat";
        _cityv6db = "/opt/rb/share/GeoIP/cityv6.dat";
        i = 0;
        try {
            _city = new LookupService(_citydb, LookupService.GEOIP_MEMORY_CACHE);
            _city6 = new LookupService(_cityv6db, LookupService.GEOIP_MEMORY_CACHE);
            _asn = new LookupService(_asndb, LookupService.GEOIP_MEMORY_CACHE);
            _asn6 = new LookupService(_asnv6db, LookupService.GEOIP_MEMORY_CACHE);

        } catch (IOException ex) {
            Logger.getLogger(GeoIpBolt.class.getName()).log(Level.SEVERE, null, ex);
        }

        geoIPevent = false;

    }

    @Override
    public void execute(Tuple tuple) {
        int topic = (int) tuple.getValueByField("topic");

        if (topic == RBEventType.EVENT) {
            event = (Map<String, Object>) tuple.getValueByField("event");
            geoIPevent = true;
        } else if (topic == RBEventType.FLOW) {
            event = (Map<String, Object>) tuple.getValueByField("flow");
            geoIPevent = true;
        }

        if (geoIPevent) {

            String ip = "";
            String where = "";
            String asnInfo;

            Location location = null;
            Location locationSrc = null;

            while (i < 2) {

                if (i == 0) {
                    ip = event.get("src").toString();
                    where = "Src";
                } else if (i == 1) {
                    ip = event.get("dst").toString();
                    where = "Dst";
                }

                Matcher match = CheckIp.VALID_IPV4_PATTERN.matcher(ip);
                if (match.matches()) {
                    location = _city.getLocation(ip);
                    asnInfo = _asn.getOrg(ip);
                } else {
                    location = _city6.getLocationV6(ip);
                    asnInfo = _asn6.getOrgV6(ip);
                }

                if (location != null) {
                    if (timeZone.timeZoneByCountryAndRegion(location.countryCode, location.region) != null) {
                        event.put("TimeZone" + where, timeZone.timeZoneByCountryAndRegion(location.countryCode, location.region));
                    }
                    if (location.countryName != null) {
                        event.put("Country" + where, location.countryName);
                    }
                    if (location.countryCode != null) {
                        event.put("CountryCode" + where, location.countryCode);
                    }
                    if (location.city != null) {
                        event.put("City" + where, location.city);
                    }
                    if (location.region != null) {
                        event.put("Region" + where, location.region);
                    }
                    if (regionName.regionNameByCode(location.countryCode, location.region) != null) {
                        event.put("RegionName" + where, regionName.regionNameByCode(location.countryCode, location.region));
                    }
                    if (location.postalCode != null) {
                        event.put("CodePostal" + where, location.postalCode);
                    }
                    if (location.latitude != 0) {
                        event.put("Latitude" + where, location.latitude);
                    }
                    if (location.longitude != 0) {
                        event.put("Longitude" + where, location.longitude);
                    }
                    if (location.metro_code != 0) {
                        event.put("Metro_Code" + where, location.metro_code);
                    }
                    if (location.dma_code != 0) {
                        event.put("Dma_Code" + where, location.dma_code);
                    }
                    if (i == 0) {
                        locationSrc = location;
                        location = null;
                    }
                    if (asnInfo != null) {
                        String[] asn = asnInfo.split(" ", 2);
                        event.put("AsnNum" + where, asn[0]);
                        event.put("AsnName" + where, asn[1]);
                    }
                }

                i++;
            }

            if (location != null && locationSrc != null) {
                if (locationSrc.distance(location) != 0) {
                    event.put("Distance", locationSrc.distance(location));
                }
            }

        }
        i = 0;
        geoIPevent = false;
        _collector.emit(new Values(event, topic));
    }

    @Override
    public void cleanup() {
        _city.close();
        _city6.close();
        _asn.close();
        _asn6.close();
    }
}
