/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.redborder.storm.util.state;

import java.io.Serializable;
import java.util.List;

public interface IStateSingleKeyBuilder extends Serializable{

	/**
	 * @return a single field primary key from this compound key
	 */
	public String buildSingleKey(List<Object> key) ;

}