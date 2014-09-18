package net.redborder.kafkastate;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class KafkaStateUpdater extends BaseStateUpdater<KafkaState> {
    private final String _messageFieldName;
    private final String _topic;

    public KafkaStateUpdater(String messageFieldName, String topic) {
        _messageFieldName = messageFieldName;
        _topic = topic;
    }

    @Override
    public void updateState(KafkaState state, List<TridentTuple> tuples,
            TridentCollector collector) {
        for (TridentTuple t : tuples) {            
            try {
                if (t.size() > 0) {
                    state.send(_topic, t.getStringByField(_messageFieldName));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
