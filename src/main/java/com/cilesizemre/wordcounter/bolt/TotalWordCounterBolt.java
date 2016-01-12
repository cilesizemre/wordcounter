package com.cilesizemre.wordcounter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class TotalWordCounterBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	
	private long counter;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		counter = 0;
	}
	
	public void execute(Tuple input) {
		counter++;
		collector.ack(input);
	}
	
	@Override
	public void cleanup() {
		System.out.println("\t->Total number of words: " + counter);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
}
