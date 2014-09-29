package net.redborder.storm.siddhi;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Created by andresgomez on 15/09/14.
 */
public class SiddhiUpdater extends BaseStateUpdater<SiddhiState> {
    @Override
    public void updateState(SiddhiState siddhiState, List<TridentTuple> tridentTuples, TridentCollector tridentCollector) {
        for(TridentTuple tuple : tridentTuples){
            siddhiState.sendToSiddhi(tuple);
        }
    }
}
