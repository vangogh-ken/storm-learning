package com.tianshouzhi.study.wordcountapp.trident.mq;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
 
import org.apache.commons.lang.BooleanUtils;
 
import backtype.storm.Config;
 
 
/**
 * Utilities for RocketMQ spout regarding its configuration and reading values
 * from the storm configuration.
 * 
 * @author Von Gosling
 */
public abstract class ConfigUtils {
	
	public static final String CONFIG_TOPIC = "rocketmq.spout.topic";
	public static final String CONFIG_CONSUMER_GROUP = "rocketmq.spout.consumer.group";
	public static final String CONFIG_TOPIC_TAG = "rocketmq.spout.topic.tag";
 
	public static final String CONFIG_ROCKETMQ = "rocketmq.config";
 
	public static final String CONFIG_PREFETCH_SIZE = "rocketmq.prefetch.size";
	
	public static final String CONFIG_NAMESRV_ADDR="public.rocketmq.domain.name";
	
	public static Properties props;
	
	static{
		 props = new Properties();
		try {
			InputStream is = ConfigUtils.class.getClassLoader().getResourceAsStream("wrestling-config.properties");
			props.load(is);
		} catch (IOException e) {
			throw new RuntimeException("load config ocurr error!", e);
		}
	}
	
	public static RocketMQConfig getRocketMQConfig() {
		String topic = props.getProperty(CONFIG_TOPIC);
		String consumerGroup = props.getProperty(ConfigUtils.CONFIG_CONSUMER_GROUP);
		String topicTag = props.getProperty(ConfigUtils.CONFIG_TOPIC_TAG);
		Integer pullBatchSize = Integer.parseInt(props.getProperty(ConfigUtils.CONFIG_PREFETCH_SIZE));
		String nameServerAddr = props.getProperty(CONFIG_NAMESRV_ADDR);
		RocketMQConfig mqConfig = new RocketMQConfig(nameServerAddr,consumerGroup, topic, topicTag);
		try {
			mqConfig.setInstanceName(consumerGroup+"_"+InetAddress.getLocalHost().getHostAddress().replaceAll("\\.", "_")+"_"+System.getProperty("worker.port"));
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		if (pullBatchSize != null && pullBatchSize > 0) {
			mqConfig.setPullBatchSize(pullBatchSize);
		}
 
		boolean ordered = BooleanUtils.toBooleanDefaultIfNull(
				Boolean.valueOf(props.getProperty("rocketmq.spout.ordered")), false);
		mqConfig.setOrdered(ordered);
		return mqConfig;
	}
 
	public static Config getTopologyConfig() {
		Config config=new Config();
		config.setNumWorkers(Integer.parseInt(props.getProperty("topology.workers")));
		config.setNumAckers(Integer.parseInt(props.getProperty("topology.acker.executors")));
		config.setMaxSpoutPending(Integer.parseInt(props.getProperty("topology.max.spout.pending")));
		config.setMessageTimeoutSecs(Integer.parseInt(props.getProperty("topology.message.timeout.secs")));
		config.put("topology.name", props.getProperty("topology.name"));
		config.setDebug(Boolean.parseBoolean(props.getProperty("topology.debug","false")));
		return config;
	}
	
	public static String get(String key){
		return props.getProperty(key);
	}
	
	public static Integer getInt(String key){
		return Integer.parseInt(props.getProperty(key));
	}
}