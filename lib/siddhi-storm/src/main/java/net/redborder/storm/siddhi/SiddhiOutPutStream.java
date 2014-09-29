package net.redborder.storm.siddhi;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by andresgomez on 22/06/14.
 */
public class SiddhiOutPutStream implements Serializable{
    SiddhiExecutionPlan _executionPlan;
    String _outPutStreamName;

    public SiddhiOutPutStream(String outPutStreamName, SiddhiExecutionPlan executionPlan){
        _executionPlan=executionPlan;
        _outPutStreamName=outPutStreamName;
        _executionPlan.outPutEventNames.put(outPutStreamName, new ArrayList<String>());
    }

    public SiddhiOutPutStream addOutPutEventName(String eventName){

        if(!_executionPlan.outPutEventNames.get(_outPutStreamName).contains(eventName)){
            _executionPlan.outPutEventNames.get(_outPutStreamName).add(eventName);
        }else{
            System.out.println("The event name: "+ eventName + " is already exists!");
        }

        return this;
    }

    public SiddhiExecutionPlan buildOutPutStream(){
        return _executionPlan;
    }
}
