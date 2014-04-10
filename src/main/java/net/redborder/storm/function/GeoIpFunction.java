/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip.timeZone;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import net.redborder.storm.util.CheckIp;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * Get the geoLocation from a IPv4 or IPv6 (sourcer and destination).
 *
 * @author andresgomez
 */
public class GeoIpFunction extends BaseFunction {

    String _citydb;
    String _countrydb;
    String _asndb;
    String _countryv6db;
    String _asnv6db;
    String _cityv6db;

    Map<String, Object> event;

    LookupService _city;
    LookupService _city6;
    LookupService _asn;
    LookupService _asn6;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

        _citydb = "/opt/rb/share/GeoIP/city.dat";
        _asndb = "/opt/rb/share/GeoIP/asn.dat";
        _asnv6db = "/opt/rb/share/GeoIP/asnv6.dat";
        _cityv6db = "/opt/rb/share/GeoIP/cityv6.dat";

        try {
            _city = new LookupService(_citydb, LookupService.GEOIP_MEMORY_CACHE);
            _city6 = new LookupService(_cityv6db, LookupService.GEOIP_MEMORY_CACHE);
            _asn = new LookupService(_asndb, LookupService.GEOIP_MEMORY_CACHE);
            _asn6 = new LookupService(_asnv6db, LookupService.GEOIP_MEMORY_CACHE);
        } catch (IOException ex) {
            Logger.getLogger(GeoIpFunction.class.getName()).log(Level.SEVERE, ex.toString());
        }
    }

    private Map<String, Object> getIPData(String ip) {

        Location location;
        Map<String, Object> eventMap = new HashMap<>();
        Matcher match = CheckIp.VALID_IPV4_PATTERN.matcher(ip);
        String asnInfo;
              
        if (match.matches()) {
            location = _city.getLocation(ip);
            asnInfo = _asn.getOrg(ip);
        } else {
            location = _city6.getLocationV6(ip);
            asnInfo = _asn6.getOrgV6(ip);
        }

        if (location != null) {
            /* if (timeZone.timeZoneByCountryAndRegion(location.countryCode, location.region) != null) {
             event.put("TimeZone" + where, timeZone.timeZoneByCountryAndRegion(location.countryCode, location.region));
             }
             if (location.countryName != null) {
             event.put("Country" + where, location.countryName);
             } */
            if (location.countryCode != null) {
                eventMap.put("country_code", location.countryCode);
            }
            /* if (location.city != null) {
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
             } */
            
            if (asnInfo != null) {
                String[] asn = asnInfo.split(" ", 2);
                // event.put("AsnNum" + where, asn[0]);
                eventMap.put("asn_name", asn[1]);
            }
        }

        //if (location != null && locationSrc != null) {
        //    if (locationSrc.distance(location) != 0) {
        //        event.put("Distance", locationSrc.distance(location));
        //     }
        //}
        
        return eventMap;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        event = (Map<String, Object>) tuple.getValueByField("event");
        Map<String, Object> geoIPMap = new HashMap<>();
        Map<String, Object> aux;
        String ip;

        if(event.containsKey("src")) {
            ip = event.get("src").toString();
            aux = getIPData(ip);
            geoIPMap.put("src_country_code", aux.get("country_code"));
            geoIPMap.put("src_as_name", aux.get("src_as_name"));
        }
        
        if(event.containsKey("dst")) {
            ip = event.get("dst").toString();
            aux = getIPData(ip);
            geoIPMap.put("dst_country_code", aux.get("country_code"));
            geoIPMap.put("dst_as_name", aux.get("src_as_name"));
        }

        collector.emit(new Values(event));
    }

    @Override
        public void cleanup() {
        _city.close();
        _city6.close();
        _asn.close();
        _asn6.close();
    }
}
