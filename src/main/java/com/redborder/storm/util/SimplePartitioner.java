/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.redborder.storm.util;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
/**
 *
 * @author andresgomez
 */
public class SimplePartitioner implements Partitioner<String> {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
 
    public int partition(String key, int a_numPartitions) {
        int partition = 0;
        int offset = key.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( key.substring(offset+1)) % a_numPartitions;
        }
       return partition;
  }
}
