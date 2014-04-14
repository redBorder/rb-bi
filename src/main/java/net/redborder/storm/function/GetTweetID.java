/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.storm.function;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author andresgomez
 */
public class GetTweetID extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> tweet = (Map<String, Object>) tuple.getValueByField("tweetMap");
        Map<String, Object> user = (Map<String, Object>) tweet.get("user");
        String id = String.valueOf(user.get("id"));
        collector.emit(new Values(id));

    }

}
