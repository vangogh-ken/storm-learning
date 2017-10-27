package com.tianshouzhi.study.wordcountapp.trident.mq;

import java.util.List;

import storm.trident.spout.IPartitionedTridentSpout.Coordinator;
 
import com.alibaba.rocketmq.common.message.MessageQueue;
 
public class RocketMQCoordinator implements Coordinator<List<MessageQueue>>{
 
	@Override
	public List<MessageQueue> getPartitionsForBatch() {
		return RocketMqConsumer.getMessageQueues();
	}
 
	@Override
	public boolean isReady(long txid) {
		return RocketMqConsumer.hasNewMesage();
	}
 
	@Override
	public void close() {
		
	}
	
}