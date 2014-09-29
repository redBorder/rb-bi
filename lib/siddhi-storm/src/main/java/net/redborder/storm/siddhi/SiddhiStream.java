package net.redborder.storm.siddhi;

import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.Serializable;

/**
 * Created by andresgomez on 20/06/14.
 */
public class SiddhiStream implements Serializable {

    StreamDefinition streamDefinition;

    SiddhiExecutionPlan _executionPlan;

    boolean _isInputStream = false;

    String source = "";


    public SiddhiStream(String streamName, SiddhiExecutionPlan executionPlan, boolean isInputStream) {
        streamDefinition  = new StreamDefinition();
        streamDefinition.name(streamName);
        _executionPlan = executionPlan;
        _isInputStream =  isInputStream;
    }

    public void setSource(String source){
        this.source=source;
    }

    public SiddhiStream addParameter (String parameterName, String parameterType){
        String type = parameterType.toLowerCase();

        if(type.contains("long")){
            streamDefinition.attribute(parameterName.toLowerCase(), Attribute.Type.LONG);
        } else if(type.contains("string")){
            streamDefinition.attribute(parameterName.toLowerCase(), Attribute.Type.STRING);
        } else if(type.contains("int") || type.contains("integer")){
            streamDefinition.attribute(parameterName.toLowerCase(), Attribute.Type.INT);
        } else if(type.contains("double")){
            streamDefinition.attribute(parameterName.toLowerCase(), Attribute.Type.DOUBLE);
        } else if(type.contains("float")){
            streamDefinition.attribute(parameterName.toLowerCase(), Attribute.Type.FLOAT);
        } else if(type.contains("bool")){
            streamDefinition.attribute(parameterName.toLowerCase(), Attribute.Type.BOOL);
        } else {
            System.out.println("Parameter: " + parameterName + " must be: {" +
                    "long|string|int|double|float|boolean");
            System.exit(1);
        }

        return this;
    }

    public SiddhiExecutionPlan buildStream(){
        _executionPlan.streams.put(streamDefinition.getStreamId(), this);
        return _executionPlan;
    }

}
