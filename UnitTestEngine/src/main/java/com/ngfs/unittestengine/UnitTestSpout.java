package com.ngfs.unittestengine;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class UnitTestSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
	private Integer i = new Integer(2);

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		// TODO Auto-generated method stub
		try {
			Timestamp timmestamp = new Timestamp(System.currentTimeMillis());

			this.collector.emit(new Values(i, sdf.format(timmestamp)));
			i += 47777777;
		} catch (Exception e) {
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("numbers", "timestamp"));
	}

}
