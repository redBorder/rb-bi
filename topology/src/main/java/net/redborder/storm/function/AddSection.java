package net.redborder.storm.function;


import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by andresgomez on 02/10/14.
 */
public class AddSection extends BaseFunction {

    String _section = "";

    public AddSection(String section){
        _section=section;
    }
    @Override
    public void execute(TridentTuple objects, TridentCollector collector) {
        collector.emit(new Values(_section));
    }
}
