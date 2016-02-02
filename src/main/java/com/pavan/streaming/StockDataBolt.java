package com.pavan.streaming;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.pavan.streaming.Topology;

/**
 *
 * @author hkropp
 */
public class StockDataBolt extends BaseBasicBolt {

    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
    	String[] colNames = {"DATE_TIME", "PRODUCT_ID", "DEVICE_INFO", "USER_ID", "TAG", "SCAN_STATUS", "SECURITY_PROCESS", "ANALYTIC_PROCESS", "ENTITY_ID", "ZIPCODE", "STATE", "COUNTRY", "CITY"};
        ofDeclarer.declare(new Fields("DATE_TIME", "PRODUCT_ID", "USER_ID", "SCAN_STATUS", "SECURITY_PROCESS", "ANALYTIC_PROCESS", 
        		"ENTITY_ID", "ZIPCODE"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        Fields fields = tuple.getFields();
        try {
            String stockDataStr = new String((byte[]) tuple.getValueByField(fields.get(0)), "UTF-8");
            String[] stockData = stockDataStr.split(",");
            /*Values values = new Values(df.parse("2016-01-31"), stockData[1],
                    stockData[3], stockData[5], stockData[6], stockData[7],
                    stockData[8], stockData[9]);*/
            Values values = new Values(df.parse("2016-01-31"), stockData[8],
            stockData[9], stockData[10], stockData[11], stockData[12],
            stockData[13], stockData[14]);
            outputCollector.emit(values);
        } catch (UnsupportedEncodingException | ParseException ex) {
            Logger.getLogger(Topology.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedException(ex.toString());
        }
    }
}
