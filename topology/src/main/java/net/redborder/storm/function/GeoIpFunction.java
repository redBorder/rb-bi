/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * <p>This enriching gets the geoLocation from a IPv4 or IPv6 (source and destination).</p>
 *
 * @author andresgomez
 */
public class GeoIpFunction extends BaseFunction {

    /**
     * Path to city data base.
     */
    public static String CITY_DB_PATH = "/opt/rb/share/GeoIP/city.dat";
    /**
     * Path to city v6 data base.
     */
    public static String CITY_V6_DB_PATH = "/opt/rb/share/GeoIP/cityv6.dat";
    /**
     * Path to asn data base.
     */
    public static String ASN_DB_PATH = "/opt/rb/share/GeoIP/asn.dat";
    /**
     * Path to asn v6 data base.
     */
    public static String ASN_V6_DB_PATH = "/opt/rb/share/GeoIP/asnv6.dat";
    /**
     * Pattern to to make the comparison with ips v4.
     */
    public static Pattern VALID_IPV4_PATTERN = null;
    /**
     * Pattern to to make the comparison with ips v6.
     */
    public static Pattern VALID_IPV6_PATTERN = null;
    /**
     * Regular expresion to make the comparison with ipv4 format.
     */
    private static final String ipv4Pattern = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
    /**
     * Regular expresion to make the comparison with ipv6 format.
     */
    private static final String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";


    /**
     * Reference on memory cache to city data base.
     */
    LookupService _city;
    /**
     * Reference on memory cache to city v6 data base.
     */
    LookupService _city6;
    /**
     * Reference on memory cache to asn data base.
     */
    LookupService _asn;
    /**
     * Reference on memory cache to asn v6 data base.
     */
    LookupService _asn6;


    /**
     * Initializing the database reference and patterns.
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        try {
            _city = new LookupService(CITY_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            _city6 = new LookupService(CITY_V6_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            _asn = new LookupService(ASN_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            _asn6 = new LookupService(ASN_V6_DB_PATH, LookupService.GEOIP_MEMORY_CACHE);
            VALID_IPV4_PATTERN = Pattern.compile(ipv4Pattern, Pattern.CASE_INSENSITIVE);
            VALID_IPV6_PATTERN = Pattern.compile(ipv6Pattern, Pattern.CASE_INSENSITIVE);
        } catch (IOException ex) {
            Logger.getLogger(GeoIpFunction.class.getName()).log(Level.SEVERE, ex.toString());
        } catch (PatternSyntaxException e) {
            Logger.getLogger(GeoIpFunction.class.getName()).log(Level.SEVERE, "Unable to compile IP check patterns");
        }
    }

    /**
     * <p>Query if there is a country code for a given IP.</p>
     * @param ip This is the address to query the data base.
     * @return The country code, example: US, ES, FR.
     */
    private String getCountryCode(String ip) {
        Matcher match = VALID_IPV4_PATTERN.matcher(ip);
        String countryCode = null;
        Location location;

        if (match.matches()) {
            location = _city.getLocation(ip);
        } else {
            location = _city6.getLocationV6(ip);
        }

        if (location != null) {
            countryCode = location.countryCode;
        }

        return countryCode;
    }

    /**
     * <p>Query if there is a asn for a given IP.</p>
     * @param ip This is the address to query the data base.
     * @return The asn name.
     */
    private String getAsnName(String ip) {
        Matcher match = VALID_IPV4_PATTERN.matcher(ip);
        String asnName = null;
        String asnInfo = null;

        if (match.matches()) {
            asnInfo = _asn.getOrg(ip);
        } else {
            asnInfo = _asn6.getOrgV6(ip);
        }

        if (asnInfo != null) {
            String[] asn = asnInfo.split(" ", 2);

            if (asn.length > 1) {
                if (asn[1] != null) asnName = asn[1];
            } else {
                if (asn[0] != null) asnName = asn[0];
            }
        }

        return asnName;
    }

    /**
     * <p>This enriching gets the geoLocation from a IPv4 or IPv6 (source and destination).</p>
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        Map<String, Object> geoIPMap = new HashMap<>();
        String src = (String) event.get("src");
        String dst = (String) event.get("dst");

        if (src != null) {
            String country_code = getCountryCode(src);
            String asn_name = getAsnName(src);

            if (country_code != null) geoIPMap.put("src_country_code", country_code);
            if (asn_name != null) geoIPMap.put("src_as_name", asn_name);
        }

        if (dst != null) {
            String country_code = getCountryCode(dst);
            String asn_name = getAsnName(dst);

            if (country_code != null) geoIPMap.put("src_country_code", country_code);
            if (asn_name != null) geoIPMap.put("src_as_name", asn_name);
        }

        collector.emit(new Values(geoIPMap));
    }

    /**
     * <p>Free the cache memory where database were loaded.</p>
     */
    @Override
    public void cleanup() {
        _city.close();
        _city6.close();
        _asn.close();
        _asn6.close();
    }
}