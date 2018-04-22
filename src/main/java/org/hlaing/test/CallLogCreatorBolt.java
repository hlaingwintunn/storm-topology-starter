package org.hlaing.test;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CallLogCreatorBolt implements IRichBolt {
	private static final long serialVersionUID = 6467345302540348182L;
	
	//Create instance for output collector which collects and emits tuples to produce output
	private OutputCollector collector;

	public void cleanup() {}

	public void execute(Tuple tuple) {
        String from = tuple.getString(0);
        String to = tuple.getString(1);
        Integer duration = tuple.getInteger(2);
        collector.emit(new Values(from + " - "+ to, duration));
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call", "duration"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
