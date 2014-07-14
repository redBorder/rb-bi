package net.redborder.storm.test.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by andresgomez on 14/07/14.
 */
public class SpoutFile extends BaseRichSpout {

    SpoutOutputCollector _collector;
    String _filePath;
    Scanner _scanner;

    public SpoutFile(String filePath){
        _filePath=filePath;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("str"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector=spoutOutputCollector;
        File file = new File(_filePath);
        try {
            _scanner = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        System.out.println("ENVIANDO!!");
        while(_scanner.hasNext()) {
            String str = _scanner.next();
            System.out.println(str);
            _collector.emit(new Values(str));
        }
    }

    @Override
    public void close() {
        _scanner.close();
    }
}
