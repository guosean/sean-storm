package com.sean.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by guozhenbin on 16/8/24.
 */
public class SeanCount extends BaseRichBolt {
    OutputCollector _collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        System.out.println("====================sean prepare=====================");
    }

    public void execute(Tuple tuple) {
        System.out.println("===============seanbolt==================");
        System.out.println("===============fields:==>"+tuple.getFields());
        _collector.ack(tuple);
        System.out.println("=========================================");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
