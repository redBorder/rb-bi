/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.filter;

import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;
import java.util.logging.Level;
import java.util.logging.Logger;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class SleepFilter extends BaseFilter{
    
    long _millis;

    
    public SleepFilter(long millis){
               _millis=millis;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        
        System.out.println("Tiempo: " + tuple.getValues().toString());
        try {
            //Utils.sleep(_millis);
            Time.sleep(_millis);
        } catch (InterruptedException ex) {
            Logger.getLogger(SleepFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }
    
}
