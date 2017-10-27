package com.tianshouzhi.study.wordcountapp.trident;

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
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class WordCountApp {
	public static void main(String[] args) {
		t2();
	}

	public static void t2() {
		// 无限的输入流中读取语句作为输入：
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
				new Values("the cow jumped over the moon"),
				new Values("the man went to the store and bought some candy"),
				new Values("four score and seven years ago"), new Values("how many apples can you eat"));
		spout.setCycle(true);

		// 这个spout会循环输出列出的那些语句到sentence stream当中，下面的代码会以这个stream作为输入并计算每个单词的个数：
		TridentTopology topology = new TridentTopology();
		TridentState wordCounts = topology.newStream("spout1", spout)
				.each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
				// persistentAggregate会帮助你把一个状态源聚合的结果存储或者更新到存储当中
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))//
				.parallelismHint(6);

		// 配置

		Config conf = new Config();
		String fileName = "words.txt";
		conf.put("fileName", fileName);
		conf.setDebug(false);

		// 运行拓扑

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, topology.build());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cluster.shutdown();
	}

	public static void t() {
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("word-reader-stream", new WordReader()).parallelismHint(16)
				.each(new Fields("line"), new NormalizeFunction(), new Fields("word")).groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Sum(), new Fields("sum"));
		StormTopology stormTopology = tridentTopology.build();

		// 配置

		Config conf = new Config();
		String fileName = "words.txt";
		conf.put("fileName", fileName);
		conf.setDebug(false);

		// 运行拓扑

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, stormTopology);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cluster.shutdown();
	}
}