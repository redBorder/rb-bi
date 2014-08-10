package net.redborder.storm.function;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.exceptions.StratioStreamingException;
import com.stratio.streaming.messaging.ColumnNameType;
import com.stratio.streaming.messaging.ColumnNameValue;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.planner.processor.TridentContext;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 31/07/14.
 */
public class StratioStreamingFunction extends BaseFunction {

    IStratioStreamingAPI stratioStreamingAPI = null;


    public void prepare(java.util.Map conf, storm.trident.operation.TridentOperationContext context) {

        ColumnNameType firstStreamColumn = new ColumnNameType("src", ColumnType.STRING);
        ColumnNameType secondStreamColumn = new ColumnNameType("bytes", ColumnType.INTEGER);
        String streamName = "stratio_flow";
        List columnList = Arrays.asList(firstStreamColumn, secondStreamColumn);

        try {
            stratioStreamingAPI = StratioStreamingAPIFactory.create().initializeWithServerConfig("pablo02", 9092, "pablo02", 2181);
        } catch (StratioEngineConnectionException e) {
            e.printStackTrace();
        }

        try {
            if (!stratioStreamingAPI.listStreams().contains(streamName))
                stratioStreamingAPI.createStream(streamName, columnList);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map<String, Object> map = (Map<String, Object>) tuple.get(0);
        String streamName = "stratio_flow";
        ColumnNameValue firstColumnValue = new ColumnNameValue("src", map.get("src"));
        ColumnNameValue secondColumnValue = new ColumnNameValue("bytes", map.get("bytes"));
        List<ColumnNameValue> streamData = Arrays.asList(firstColumnValue, secondColumnValue);
        try {
            stratioStreamingAPI.insertData(streamName, streamData);
        } catch (StratioStreamingException ssEx) {
            ssEx.printStackTrace();
        }
    }
}
