/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.redborder.storm.trident.filter;

import backtype.storm.utils.Utils;
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
        Utils.sleep(_millis);       
        return true;
    }
    
}
