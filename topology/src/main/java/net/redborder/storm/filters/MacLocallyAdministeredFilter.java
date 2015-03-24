package net.redborder.storm.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.math.BigInteger;
import java.util.Map;

/**
 * Created by andresgomez on 14/1/15.
 */
public class MacLocallyAdministeredFilter extends BaseFilter {

    boolean _debug = false;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _debug = (boolean) conf.get("rbDebug");
    }

    public String hexToBin(String s) {
        return new BigInteger(s, 16).toString(2);
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        String mac = tuple.getStringByField("src_mac");
        boolean status = true;

        if(mac!=null) {
            String macSplit[] = mac.split(":");
            String macHex = macSplit[0];

            // if (_debug)
            //    System.out.println("HEX: " + macHex);

            String binary = hexToBin(macHex);

            if (binary.endsWith("10") || binary.endsWith("11"))
                status = false;

            // if (_debug) {
            //     System.out.println("BIN: " + binary);
            //     System.out.println("TRUE/FALSE: " + status);
            // }
        }
        return status;
    }

}
