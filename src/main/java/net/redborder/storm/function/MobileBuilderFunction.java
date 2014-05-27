/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Make a java.util.Map from the json string.
 *
 * @author andresgomez
 */
public class MobileBuilderFunction extends BaseFunction {

    private Map<String, Object> hnb_register(Document document) {
        Map<String, Object> event = new HashMap<>();
        
        try {
            String hnbid = document.getElementsByTagName("hnbid").item(0).getTextContent();
            String location = document.getElementsByTagName("location").item(0).getTextContent();
            String hnbGeoLocation = document.getElementsByTagName("hnbGeoLocation").item(0).getTextContent();
            event.put("wireless_station", hnbid);
            event.put("hnblocation", location);
            event.put("hnbgeolocation", hnbGeoLocation);
        } catch (NullPointerException ex) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed reading a UE IP Assign message", ex);
        }
        
        return event;
    }
    
    private Map<String, Object> ue_register(Document document) {
        Map<String, Object> event = new HashMap<>();
        
        try {
            String imsi = document.getElementsByTagName("imsi").item(0).getTextContent();
            event.put("client_id", imsi);
        } catch (NullPointerException ex) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed reading a UE Register message", ex);
        }
        
        return event;
    }
    
    private Map<String, Object> ue_ip_assign(Document document) {
        Map<String, Object> event = new HashMap<>();
        
        try {
            String imsi = document.getElementsByTagName("imsi").item(0).getTextContent();
            String apn = document.getElementsByTagName("apn").item(0).getTextContent();
            String ipAddress = document.getElementsByTagName("ipAddress").item(0).getTextContent();
            String rat = document.getElementsByTagName("rat").item(0).getTextContent();
            event.put("client_id", imsi);
            event.put("wireless_id", apn);
            event.put("ip", ipAddress);
            event.put("rat", rat);
        } catch (NullPointerException ex) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed reading a UE IP Assign message", ex);
        }
        
        return event;
    }    
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        String type, tag, path;
        String key = null;
        DocumentBuilderFactory factory;
        DocumentBuilder builder;
        Document document;

        String xml = (String) tuple.getValue(0);
        Map<String, Object> event = null;

        try {
            factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            builder = factory.newDocumentBuilder();
            document = builder.parse(new ByteArrayInputStream(xml.getBytes()));
            document.getDocumentElement().normalize();

            tag = document.getDocumentElement().getNodeName();
            type = ((Attr) document.getDocumentElement().getAttributes().getNamedItem("type")).getValue();
            path = ((Attr) document.getDocumentElement().getAttributes().getNamedItem("path")).getValue();

            if (tag.equals("feed") && type.equals("add")) {
                if (document.getElementsByTagName("hnbid").getLength() > 0) {
                    event = hnb_register(document);
                    key = path;
                } else {
                    event = ue_ip_assign(document);
                    key = (String) event.get("ip");
                }
            } else if (tag.equals("notify") && type.equals("add")) {
                event = ue_register(document);
                event.put("path", path);
                key = (String) event.get("client_id");
            }              
        } catch (ParserConfigurationException | SAXException | IOException | NullPointerException ex) {
            Logger.getLogger(GetMSEdata.class.getName()).log(Level.SEVERE, "Failed reading a Mobile XML tuple", ex);
        }

        if (event != null && key != null) {
            collector.emit(new Values(key, event));
        }
    }

}
