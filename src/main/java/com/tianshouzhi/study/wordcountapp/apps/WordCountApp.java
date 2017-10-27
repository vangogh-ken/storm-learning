package com.tianshouzhi.study.wordcountapp.apps;

import com.tianshouzhi.study.wordcountapp.bolts.WordCounter;
import com.tianshouzhi.study.wordcountapp.bolts.WordNormalizer;
import com.tianshouzhi.study.wordcountapp.spouts.WordReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;

public class WordCountApp {
	public static void main(String[] args)
			throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		// 定义拓扑
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader(), 3);//读3次
		builder.setBolt("word-normalizer", new WordNormalizer(), 3).shuffleGrouping("word-reader");//3个bolt接收处理
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-normalizer", new Fields("word"));
		StormTopology topology = builder.createTopology();
		// 配置

		Config conf = new Config();
		String fileName = "words.txt";
		conf.put("fileName", fileName);
		conf.setDebug(false);

		// 运行拓扑

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, topology);
		Thread.sleep(5000);
		cluster.shutdown();

	}
}