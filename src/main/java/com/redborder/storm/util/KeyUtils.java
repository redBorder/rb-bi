/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.redborder.storm.util;


import java.util.Arrays;
import java.util.List;

public class KeyUtils {

	@SuppressWarnings("unchecked")
	public static List<List<Object>> toKeys(Object singleKey) {
		List<List<Object>> keys = Arrays.asList(Arrays.asList(singleKey));
		return keys;
	}
}
