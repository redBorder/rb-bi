package com.github.quintona;

import java.util.List;
import kafka.producer.KeyedMessage;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class KafkaStateUpdater extends BaseStateUpdater<KafkaState> {

    private String messageFieldName;

    public KafkaStateUpdater(String messageFieldName) {
        this.messageFieldName = messageFieldName;
    }

    @Override
    public void updateState(KafkaState state, List<TridentTuple> tuples,
            TridentCollector collector) {
        for (TridentTuple t : tuples) {
            try {
                if (t.size() > 0) {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>("rb_flow_post", t.getStringByField(messageFieldName));
                    state.enqueue(data);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
