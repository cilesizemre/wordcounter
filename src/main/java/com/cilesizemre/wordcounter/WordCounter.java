package com.cilesizemre.wordcounter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.cilesizemre.wordcounter.bolt.NotStopWordCounterBolt;
import com.cilesizemre.wordcounter.bolt.StopWordCounterBolt;
import com.cilesizemre.wordcounter.bolt.TotalWordCounterBolt;
import com.cilesizemre.wordcounter.bolt.WordCounterBolt;
import com.cilesizemre.wordcounter.spout.WordSpout;

public class WordCounter {
	
	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		config.put("inputFile", args[0]);
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-spout", new WordSpout());
		builder.setBolt("word-counter", new WordCounterBolt()).fieldsGrouping("word-spout", new Fields("word"));
		builder.setBolt("total-word-counter", new TotalWordCounterBolt()).shuffleGrouping("word-counter");
		builder.setBolt("stop-word-counter", new StopWordCounterBolt()).shuffleGrouping("word-counter");
		builder.setBolt("not-stop-word-counter", new NotStopWordCounterBolt()).shuffleGrouping("word-counter");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCounter", config, builder.createTopology());
		
		Utils.sleep(10000);
		cluster.shutdown();
	}
	
}
