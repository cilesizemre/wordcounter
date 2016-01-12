package com.cilesizemre.wordcounter.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class WordSpout implements IRichSpout {
	
	private SpoutOutputCollector collector;
	private Scanner scanner;
	private Pattern regex;
	private Locale locale;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		String inputFile = conf.get("inputFile").toString();
		try {
			Reader reader = new BufferedReader(new FileReader(inputFile));
			this.scanner = new Scanner(reader);
			this.scanner.useDelimiter(Pattern.compile("[\\s]*"));
			this.regex = Pattern.compile("[a-zA-Z0-9]+", Pattern.MULTILINE | Pattern.DOTALL);
			this.locale = new Locale("en");
		}
		catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file " + conf.get("inputFile"));
		}
	}
	
	public void nextTuple() {
		if (scanner.findWithinHorizon(regex, 0) != null) {
			String word = scanner.match().group().toLowerCase(locale);
			this.collector.emit(new Values(word), word);
		}
	}
	
	public void close() {
		scanner.close();
	}
	
	public void activate() {
		
	}
	
	public void deactivate() {
		
	}
	
	public void ack(Object msgId) {
		
	}
	
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
