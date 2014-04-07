/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.redborder.storm.util.state;

import java.util.List;


/**
 * Basic {@link IStateSingleKeyBuilder} that just concatenates parts of a compound key into a string 
 *
 */
public class ConcatKeyBuilder implements IStateSingleKeyBuilder {

	private static final long serialVersionUID = 1L;

	// prefix assigned to each key => use this to keep key distinct if several parts of the topology write to the same memcached 
	private final String prefix;		

	private final String nullKey;
	private final String sep ;

	public ConcatKeyBuilder() {
		this("");
	}
	public ConcatKeyBuilder(String prefix) {
		this(prefix, "_");
	}
	public ConcatKeyBuilder(String prefix, String sep) {
		this (prefix, sep, "nullkey");
	}

	public ConcatKeyBuilder(String prefix, String sep, String nullKey) {
		if (prefix == null) {
			this.prefix = "";
		} else if ("".equals(prefix)) {
			this.prefix = "";
		} else {
			this.prefix = prefix + sep;
		}
		this.nullKey = nullKey;
		this.sep = sep;
	}

	@Override
	public String buildSingleKey(List<Object> key) {

		if (key == null || key.isEmpty()) {
			return nullKey;
		} else {
			StringBuffer sb = new StringBuffer(prefix);
			for (Object o: key) {
				sb.append(o).append(sep);
			}
			return sb.toString();
		}
	}

}
