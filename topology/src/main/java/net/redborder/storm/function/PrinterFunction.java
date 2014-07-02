/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class PrinterFunction extends BaseFunction {

    String _str = "";

    public PrinterFunction(String str) {
        _str = str;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        List<Object> list = tuple.getValues();
        for (Object o : list) {

            System.out.println(_str + " " + o.toString());
        }

    }

}
