package com.cilesizemre.wordcounter.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class WordCounterBolt implements IRichBolt {
	
	private OutputCollector collector;
	
	Map<String, Integer> counters;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counters = new HashMap<String, Integer>();
	}
	
	public void execute(Tuple input) {
		String word = input.getString(0);
		if (!counters.containsKey(word)) {
			counters.put(word, 1);
		} else {
			Integer counter = counters.get(word) + 1;
			counters.put(word, counter);
		}
		collector.emit(new Values(word));
		collector.ack(input);
	}
	
	public void cleanup() {
		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
			System.out.println("\t- " + entry.getKey() + ": " + entry.getValue());
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
