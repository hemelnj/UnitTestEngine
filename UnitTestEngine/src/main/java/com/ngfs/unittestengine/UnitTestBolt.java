package com.ngfs.unittestengine;

import java.io.PrintWriter;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class UnitTestBolt extends BaseRichBolt {
	
	private PrintWriter writer;
	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Integer numbers = input.getIntegerByField("numbers");
		String timeStamp = input.getStringByField("timestamp");
		this.collector.emit(new Values(numbers,timeStamp));
		writer.println(numbers+","+timeStamp);

	}

	@Override
	public void prepare(Map stormConf, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
		String fileName = stormConf.get("fileToWrite").toString();
		try {
			writer = new PrintWriter(fileName,"UTF-8");
		} catch (Exception e) {
			throw new RuntimeException("Error Opening file ["+fileName+"]");
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("numbers", "timestamp"));
	}

	public void cleanup() {
		writer.close();
	}

}
