/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.function;

import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class AnalizeHttpUrlFunction extends BaseFunction{

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> event = (Map<String, Object>) tuple.getValue(0);
        
        
    }
    
}
