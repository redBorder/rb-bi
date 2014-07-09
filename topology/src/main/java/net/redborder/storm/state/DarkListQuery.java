package net.redborder.storm.state;

import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 09/07/14.
 */
public class DarkListQuery extends BaseQueryFunction<MapState<Map<String, Map<String, Object>>>, Map<String, Object>> {

    private boolean _debug;


    @Override
    public List<Map<String, Object>> batchRetrieve(MapState<Map<String, Map<String, Object>>> state, List<TridentTuple> tuples) {
        List<Map<String, Map<String, Object>>> darkListData = null;
        Map<String, Map<String, Object>> queryData = null;
        List<Map<String, Object>> result = new ArrayList<>();
        List<Object> keysToRequest = new ArrayList<>();
        List<String> keysToAppend = new ArrayList<>();

        for (TridentTuple t : tuples) {
            Map<String, Object> flow = (Map<String, Object>) t.getValue(0);
            String src = (String) flow.get("src");
            String dst = (String) flow.get("dst");

            if (src != null) {
                keysToAppend.add(src);

                if (!keysToRequest.contains(src)) {
                    keysToRequest.add(src);
                }
            } else {
                keysToAppend.add(null);
            }


            if (dst != null) {
                keysToAppend.add(src);

                if (!keysToRequest.contains(dst)) {
                    keysToRequest.add(dst);
                }
            } else {
                keysToAppend.add(null);
            }
        }

        if (_debug) {
            System.out.println("BatchSize " + tuples.size()
                    + " RequestedToGridGain: " + (keysToRequest.size()));
        }

        if (!keysToRequest.isEmpty()) {
            List<List<Object>> keysToGridgain = new ArrayList<>();

            for (Object key : keysToRequest) {
                List<Object> l = new ArrayList<>();
                l.add(key);
                keysToGridgain.add(l);
            }

            try {
                darkListData = state.multiGet(keysToGridgain);
                if (darkListData != null) {
                    queryData = darkListData.get(0);

                    if (_debug) {
                        System.out.println("GridGain response: " + darkListData.toString());
                    }
                }
            } catch (ReportedFailedException e) {
                Logger.getLogger(StateQuery.class.getName()).log(Level.WARNING, null, e);
            }
        }

        for (TridentTuple t : tuples) {
            Map<String, Object> flow = (Map<String, Object>) t.getValue(0);

            if (darkListData != null && !darkListData.isEmpty()) {
                Map<String, Object> mapToSave = new HashMap<>();

                String src = (String) flow.get("src");
                String dst = (String) flow.get("dst");

                Map<String, Object> srcData = queryData.get(src);
                Map<String, Object> dstData = queryData.get(dst);


                Double srcScore = 0.00;
                Double dstScore = 0.00;

                if (srcData != null)
                    srcScore = Double.parseDouble(srcData.get("darklist_score").toString());

                if (dstData != null)
                    dstScore = Double.parseDouble(dstData.get("darklist_score").toString());


                if (srcData != null && dstData != null) {
                    mapToSave.put("darklist_category_src", srcData.get("darklist_category"));
                    mapToSave.put("darklist_score_src", srcScore);
                    mapToSave.put("darklist_score_dst", dstScore);
                    mapToSave.put("darklist_score_name_src", srcData.get("darklist_score_name"));
                    mapToSave.put("darklist_protocol_src", srcData.get("darklist_protocol"));
                    mapToSave.put("darklist_category_dst", dstData.get("darklist_category"));
                    mapToSave.put("darklist_protocol_dst", dstData.get("darklist_protocol"));
                    mapToSave.put("darklist_score_name_dst", dstData.get("darklist_score_name"));
                    mapToSave.put("darklist_category", srcData.get("darklist_category").toString() + "/" + dstData.get("darklist_category"));
                    mapToSave.put("darklist_protocol", srcData.get("darklist_protocol").toString() + "/" + dstData.get("darklist_protocol"));
                   // mapToSave.put("darklist_score_name", );
                    mapToSave.put("darklist_direction", "Both");
                } else if (srcData != null) {
                    mapToSave.put("darklist_score_src", srcScore);
                    mapToSave.put("darklist_score_dst", dstScore);
                    mapToSave.put("darklist_category_src", srcData.get("darklist_category"));
                    mapToSave.put("darklist_protocol_src", srcData.get("darklist_protocol"));
                    mapToSave.put("darklist_category_dst", "Clean");
                    mapToSave.put("darklist_protocol_dst", "Clean");
                    //mapToSave.put("darklist_score_name", );
                    mapToSave.put("darklist_protocol", srcData.get("darklist_protocol"));
                    mapToSave.put("darklist_category", srcData.get("darklist_category"));
                    mapToSave.put("darklist_direction", "Source");
                } else if (dstData != null) {
                    mapToSave.put("darklist_score_src", srcScore);
                    mapToSave.put("darklist_score_dst", dstScore);
                    //mapToSave.put("darklist_score_name", );
                    mapToSave.put("darklist_category_dst", dstData.get("darklist_category"));
                    mapToSave.put("darklist_protocol_dst", dstData.get("darklist_protocol"));
                    mapToSave.put("darklist_category_src", "Clean");
                    mapToSave.put("darklist_protocol_src", "Clean");
                    mapToSave.put("darklist_protocol", dstData.get("darklist_protocol"));
                    mapToSave.put("darklist_category", dstData.get("darklist_category"));
                    mapToSave.put("darklist_direction", "Destination");
                } else {
                    mapToSave.put("darklist_score_src", srcScore);
                    mapToSave.put("darklist_score_dst", dstScore);
                    mapToSave.put("darklist_score_name", "Clean");
                    mapToSave.put("darklist_category_dst", "Clean");
                    mapToSave.put("darklist_protocol_dst", "Clean");
                    mapToSave.put("darklist_category_src", "Clean");
                    mapToSave.put("darklist_protocol_src", "Clean");
                    mapToSave.put("darklist_protocol", "Clean");
                    mapToSave.put("darklist_category", "Clean");
                }

                result.add(mapToSave);
            } else {

                Map<String, Object> mapToSave = new HashMap<>();

                mapToSave.put("darklist_score_name", "clean");
                mapToSave.put("darklist_category_dst", "clean");
                mapToSave.put("darklist_protocol_dst", "clean");
                mapToSave.put("darklist_category_src", "clean");
                mapToSave.put("darklist_protocol_src", "clean");
                mapToSave.put("darklist_protocol", "clean");
                mapToSave.put("darklist_category", "clean");

                result.add(mapToSave);
            }
        }

        return result;
    }

    @Override
    public void execute(TridentTuple objects, Map<String, Object> result, TridentCollector collector) {
        collector.emit(new Values(result));
    }
}
