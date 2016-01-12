package com.cilesizemre.wordcounter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.cilesizemre.wordcounter.util.EnglishStopwordUtil;

@SuppressWarnings("serial")
public class StopWordCounterBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	
	private long counter;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		counter = 0;
	}
	
	public void execute(Tuple input) {
		String word = input.getString(0);
		if (EnglishStopwordUtil.isStopword(word)) {
			counter++;
		}
		collector.ack(input);
	}
	
	@Override
	public void cleanup() {
		System.out.println("\t->Total number of stop words: " + counter);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
}
