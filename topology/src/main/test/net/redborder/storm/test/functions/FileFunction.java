package net.redborder.storm.test.functions;


import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Created by andresgomez on 14/07/14.
 */
public class FileFunction extends BaseFunction {

    String _file;

    public FileFunction(String file){
        _file=file;
    }

    PrintWriter pw = null;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);

        FileWriter fichero = null;
        try
        {
            fichero = new FileWriter(_file);
            pw = new PrintWriter(fichero);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        pw.println(tuple.getString(0));
    }
}
