package net.redborder.storm.siddhi;

import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 19/06/14.
 */
public class SiddhiExecutionPlan implements Serializable {

    public Map<String, SiddhiStream> streams;
    public Map<String, String> querys;
    public Map<String, List<String>> outPutEventNames;
    public List<String> inputStreamName;
    public List<String> outputStreamName;
    public String _hazelCastInstance;


    public SiddhiExecutionPlan(String hazelCastInstance) {
        streams = new HashMap<String, SiddhiStream>();
        querys = new HashMap<String, String>();
        outputStreamName = new ArrayList<String>();
        outPutEventNames = new HashMap<String, List<String>>();
        inputStreamName = new ArrayList<String>();
        _hazelCastInstance = hazelCastInstance;
    }

    private SiddhiExecutionPlan setInputStreamName(String streamName, String source) {
        inputStreamName.add(streamName+":"+source);
        return this;
    }

    public SiddhiOutPutStream addOutputStreamName(String streamName) {
        outputStreamName.add(streamName);
        return new SiddhiOutPutStream(streamName, this);
    }

    public SiddhiStream newStream(String streamName) {

        return new SiddhiStream(streamName, this, false);
    }

    public SiddhiStream newInputStream(String streamName, String source) {

        setInputStreamName(streamName, source);
        SiddhiStream stream = new SiddhiStream(streamName, this, true);
        stream.setSource(source);
        return stream;

    }

    public SiddhiExecutionPlan addQuery(String queryName, String query) {

        querys.put(queryName, query);

        return this;
    }

    public String toString() {
        String toString = "";
        for (SiddhiStream stream : streams.values()) {
            toString = toString + "- Streams: [" + stream.streamDefinition.getStreamId() + "] \n";
            for (Attribute attribute : stream.streamDefinition.getAttributeList()) {
                toString = toString + "    * " + attribute.getName() + " : " + attribute.getType().name() + "\n";
            }
        }

        toString = toString + "- InputStreams: [" + inputStreamName.toString() + "] \n";
        for (String outPutStreamName : outputStreamName) {
            toString = toString + "- OutputStreams: [" + outputStreamName.toString() + "] \n";
            for (String outPutName : outPutEventNames.get(outPutStreamName)) {
                toString = toString + "    * Attribute: " + outPutName + "\n";
            }
        }

        for (int i = 0; i < querys.size(); i++) {
            toString = toString + "- Query[" + i + "]:\n";
            toString = toString + "   " + querys.get(i) + "\n";
            toString = toString + "--------------------------------\n";
        }

        return toString;
    }

}
