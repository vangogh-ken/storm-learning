package com.tianshouzhi.study.wordcountapp.trident.mq;

import java.util.List;
import java.util.Map;
 
 
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
 
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.shade.com.alibaba.fastjson.JSONObject;
 
@SuppressWarnings("rawtypes")
public class RocketMqIPatitionedTridentSpout implements IPartitionedTridentSpout<List<MessageQueue>, ISpoutPartition, JSONObject>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4572682097086392244L;
	
	@Override
	//这个方法会被调用多次，不适合初始化consumer
	public storm.trident.spout.IPartitionedTridentSpout.Coordinator<List<MessageQueue>> getCoordinator(
			Map conf, TopologyContext context) {
		return new RocketMQCoordinator();
	}
 
	@Override
	public storm.trident.spout.IPartitionedTridentSpout.Emitter<List<MessageQueue>, ISpoutPartition, JSONObject> getEmitter(
			Map conf, TopologyContext context) {
		return new RocketMQEmitter();
	}
 
	
	@Override
	public Map getComponentConfiguration() {
		
		return null;
	}
 
	@Override
	//这个方法也会被调用多次，不是初始化
	public Fields getOutputFields() {
		
		return new Fields("tId", "message");
	}
}