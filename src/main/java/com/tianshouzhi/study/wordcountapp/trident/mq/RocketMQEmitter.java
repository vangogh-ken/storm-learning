package com.tianshouzhi.study.wordcountapp.trident.mq;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout.Emitter;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.tuple.Values;

import com.alibaba.dubbo.common.utils.LRUCache;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.shade.com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

public class RocketMQEmitter implements Emitter<List<MessageQueue>, ISpoutPartition, JSONObject> {
	private static final Logger logger = LoggerFactory.getLogger(RocketMQEmitter.class);
	// 保证客户端幂等性
	private volatile LRUCache<String, MessageExt> idempotentCache = null;
	{
		idempotentCache = new LRUCache<String, MessageExt>();
		idempotentCache.setMaxCapacity(10000);
	}

	@Override
	public List<ISpoutPartition> getOrderedPartitions(List<MessageQueue> allPartitionInfo) {
		List<ISpoutPartition> partitions = null;
		if (allPartitionInfo != null && allPartitionInfo.size() > 0) {
			partitions = Lists.newArrayList();
			for (final MessageQueue messageQueue : allPartitionInfo) {
				partitions.add(new ISpoutPartition() {
					@Override
					public String getId() {
						return RocketMqConsumer.makeMessageQueueUniqueId(messageQueue);
					}
				});
			}
		}
		if (partitions == null || partitions.size() == 0) {
			throw new RuntimeException("partitions is null");
		}
		String partitionsStr = mkpartitionsStr(partitions);
		logger.info("all partitions,{}", partitionsStr);
		return partitions;
	}

	private String mkpartitionsStr(List<ISpoutPartition> partitions) {
		StringBuilder builder = new StringBuilder();
		for (ISpoutPartition iSpoutPartition : partitions) {
			builder.append("\n" + iSpoutPartition.getId());
		}
		return builder.toString();
	}

	/**
	 * 从指定分区中获取数据，注意partition和lastPartitionMeta两个是一一对应的
	 * lastPartitionMeta表示的是当前传入的partition的分区元数据
	 */
	@Override
	public JSONObject emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector,
			ISpoutPartition partition, JSONObject lastPartitionMeta) {

		// 1、根据partitionId获取对应的消息队列
		MessageQueue mq = RocketMqConsumer.getMessageQueueByUniqueId(partition.getId());

		// 2、获取队列的当前消费进度offset，先从lastPartitionMeta取，lastPartitionMeta=null，连接远程获取
		long beginOffset = 0;
		if (lastPartitionMeta != null) {
			Object object = lastPartitionMeta.get(RocketMqPartitionMeta.NEXT_OFFSET);
			if (object instanceof String) {
				beginOffset = Long.parseLong((String) object);
			} else if (object instanceof Long) {
				beginOffset = (Long) object;
			}
		} else {
			try {
				logger.info("queue:{},lastPartitionMeta is null", partition.getId());
				beginOffset = RocketMqConsumer.getConsumer().fetchConsumeOffset(mq, true);
				beginOffset = (beginOffset == -1) ? 0 : beginOffset;
			} catch (MQClientException e) {
				logger.error("fetch queue offset error ,queue:" + mq, e);
				return lastPartitionMeta;
			}
		}
		int batchSize = 0;
		// 3、获取消息并处理
		PullResult pullResult;
		try {
			pullResult = RocketMqConsumer.getConsumer().pull(mq, RocketMqConsumer.getRocketMQConfig().getTopicTag(),
					beginOffset, RocketMqConsumer.getRocketMQConfig().getPullBatchSize());
			PullStatus pullStatus = pullResult.getPullStatus();
			switch (pullStatus) {
			case FOUND:
				logger.info("queue:{},found new msgs,pull result{}:", partition.getId(), pullResult);
				List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
				if (msgFoundList != null && msgFoundList.size() > 0) {
					batchSize = msgFoundList.size();
					for (MessageExt messageExt : msgFoundList) {
						if (!idempotentCache.containsKey(messageExt.getMsgId())) {
							String msgContent = new String(messageExt.getBody());
							collector.emit(new Values(tx, msgContent));
							idempotentCache.put(messageExt.getMsgId(), messageExt);
							System.out.println("emit message:" + messageExt + ",message content:" + msgContent);
						} else {
							logger.warn("message {} has consumed!", messageExt);
						}

					}
				}
				break;
			case OFFSET_ILLEGAL:// 当beginOffset小于Topic队列的minOffset，会出现此问题
				logger.warn("OFFSET_ILLEGAL ,Message Queue:{},pullReuslt:{},caculate beginOffset:{}",
						new Object[] { mq, pullResult, beginOffset });
				break;
			case NO_NEW_MSG:
				break;
			case NO_MATCHED_MSG:// 当队列中存在其他Tag的消息时，出现此情况
				logger.warn("May be some msg has other tag exsits in the queue:{},pull status:{}", mq, pullStatus);
				break;
			default:
				logger.warn("UNKNOW STATUS:{},Message Queue:{}", pullStatus, mq);
				break;
			}
			// 不管pullStatus的状态如何，都更新ConsumeOffset
			RocketMqConsumer.getConsumer().updateConsumeOffset(mq, pullResult.getNextBeginOffset());
		} catch (Exception e) {
			logger.error("pull message error,topic:" + RocketMqConsumer.getRocketMQConfig().getTopic() + ",queue:" + mq,
					e);
			return lastPartitionMeta;
		}

		// 4、更新PartitionMeta
		RocketMqPartitionMeta rocketMqPartitionMeta = new RocketMqPartitionMeta(partition.getId(), tx.toString(),
				beginOffset, pullResult.getNextBeginOffset());
		rocketMqPartitionMeta.setBatchSize(batchSize);
		
		return rocketMqPartitionMeta;
	}

	@Override
	public void refreshPartitions(List<ISpoutPartition> partitionResponsibilities) {
		logger.info("refreshPartitions");

	}

	@Override
	public void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, ISpoutPartition partition,
			JSONObject partitionMeta) {
	}

	@Override
	public void close() {

	}
}