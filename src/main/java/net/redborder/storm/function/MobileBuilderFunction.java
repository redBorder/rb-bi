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

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        
        String path, type, imsi, ipAddress, rat;
        DocumentBuilderFactory factory;
        DocumentBuilder builder;
        Document document;

        String xml = (String) tuple.getValue(0);
        Map<String, Object> event = new HashMap<>();

        try {
            factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            builder = factory.newDocumentBuilder();
            document = builder.parse(new ByteArrayInputStream(xml.getBytes()));
            document.getDocumentElement().normalize();
        } catch (ParserConfigurationException | SAXException | IOException ex) {
            Logger.getLogger(MobileBuilderFunction.class.getName()).log(Level.SEVERE, ex.toString());
            return;
        }
        
        try {
            path = ((Attr) document.getDocumentElement().getAttributes().getNamedItem("path")).getValue();
            type = ((Attr) document.getDocumentElement().getAttributes().getNamedItem("type")).getValue();
            imsi = document.getElementsByTagName("imsi").item(0).getTextContent();
            ipAddress = document.getElementsByTagName("ipAddress").item(0).getTextContent();
            rat = document.getElementsByTagName("rat").item(0).getTextContent();
        } catch (NullPointerException ex) {
            Logger.getLogger(MobileBuilderFunction.class.getName()).log(Level.SEVERE, ex.toString());
            return;
        }

        event.put("path", path);
        event.put("type", type);
        event.put("imsi", imsi);
        event.put("ipAddress", ipAddress);
        event.put("rat", rat);

        collector.emit(new Values(event));
        
    }

}
