package com.sean.storm;

import com.google.common.io.Files;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by guozhenbin on 16/8/24.
 */
public class SeanCount extends BaseRichBolt {
    OutputCollector _collector;
    static final String fileName = "/users/guozhenbin/tmp/out.storm";

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        System.out.println("====================sean prepare=====================");
    }

    public void execute(Tuple tuple) {
        File file = new File(fileName);
        try {
            Files.append("===============seanbolt==================\n", file, Charset.defaultCharset());
            System.out.println("===============fields:==>" + tuple.getFields());

            Files.append(tuple.getStringByField("word")+"\n", file, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }

        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
