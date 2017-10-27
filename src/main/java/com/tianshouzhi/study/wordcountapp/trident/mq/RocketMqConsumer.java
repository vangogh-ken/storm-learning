package com.tianshouzhi.study.wordcountapp.trident.mq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.google.common.collect.Sets;
 
public abstract class RocketMqConsumer {
	
	private static final Logger logger=LoggerFactory.getLogger(RocketMqConsumer.class);
	private static DefaultMQPullConsumer pullConsumer;
	public static boolean initialized=false;
	public static RocketMQConfig rocketMQConfig;
	private static Map<String,MessageQueue> partitionIdQueueMap;
	private static List<MessageQueue> queues;
	
	static{
		initRocketMqConfig();
		initConsumer();
		initMessageQueues();
		registerMessageQueueListener();
	}
	
	private static void initRocketMqConfig() {
		rocketMQConfig = ConfigUtils.getRocketMQConfig();
	}
	
	private static void initConsumer() {
		pullConsumer=new DefaultMQPullConsumer();
		pullConsumer.setConsumerGroup(rocketMQConfig.getGroupId());
		pullConsumer.setInstanceName(rocketMQConfig.getInstanceName());
		pullConsumer.setNamesrvAddr(rocketMQConfig.getNamesrvAddr());
		pullConsumer.setRegisterTopics(Sets.newHashSet(rocketMQConfig.getTopic()));
		
		logger.info("rocketmq pullConsumer config:{}",rocketMQConfig);
		
		try {
			pullConsumer.start();
			logger.info("rocketmq pullConsumer startup success!");
		} catch (MQClientException e) {
			throw new RuntimeException("consumer start fail!",e);
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
 
			@Override
			public void run() {
				if (pullConsumer != null) {
					pullConsumer.shutdown();
					logger.info("rocketmq pullConsumer shutdown success!");
				}
			}
			
		});
	}
	
	private static void initMessageQueues() {
		try {
			//因为队列可能扩容，每次开启一个新的事务(batch)时，都重新拉取一下最新的队列
			Set<MessageQueue> messageQueues = RocketMqConsumer.getConsumer().fetchSubscribeMessageQueues(RocketMqConsumer.getRocketMQConfig().getTopic());
			queues=new ArrayList<MessageQueue>(messageQueues);
			partitionIdQueueMap=new HashMap<String,MessageQueue>();
			for (MessageQueue messageQueue : messageQueues) {
				partitionIdQueueMap.put(makeMessageQueueUniqueId(messageQueue), messageQueue);
			}
		} catch (MQClientException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
		
	}
	
	private static void registerMessageQueueListener() {
		pullConsumer.registerMessageQueueListener(rocketMQConfig.getTopic(),new MessageQueueListener() {
			
			@Override
			public void messageQueueChanged(String topic, Set<MessageQueue> mqAll,
					Set<MessageQueue> mqDivided) {
				initMessageQueues();
			}
		} );
	}
	
	public static DefaultMQPullConsumer getConsumer(){
		return pullConsumer;
	}
	
	
 
	
	public static String makeMessageQueueUniqueId(MessageQueue messageQueue) {
		/*String brokerName = messageQueue.getBrokerName();
		String topic = messageQueue.getTopic();
		int queueId = messageQueue.getQueueId();*/
		return messageQueue.getBrokerName()+"-queue-"+messageQueue.getQueueId();
		
	}
	
	
	public static RocketMQConfig getRocketMQConfig(){
		return rocketMQConfig;
	}
	
	public static List<MessageQueue> getMessageQueues(){
		return queues;
	}
	
	public static MessageQueue getMessageQueueByUniqueId(String uniqueId){
		return partitionIdQueueMap.get(uniqueId);
	}
	
	public static boolean hasNewMesage(){
		/*try{
			for (MessageQueue messageQueue : queues) {
				long offset = pullConsumer.fetchConsumeOffset(messageQueue, true);
				offset=(offset<0)?0:offset;
				PullResult pullResult = pullConsumer.pull(messageQueue, rocketMQConfig.getTopicTag(), offset, rocketMQConfig.getPullBatchSize());
				PullStatus pullStatus = pullResult.getPullStatus();
				switch (pullStatus) {
				case FOUND:
					return true;
				case OFFSET_ILLEGAL:
					pullConsumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
					logger.warn("OFFSET_ILLEGAL,Queue:{},PullResult:{}",messageQueue,pullResult);break;
				case NO_NEW_MSG:break;
				case NO_MATCHED_MSG:
					logger.warn("May be some msg has other tag exsits in the queue:{},pull status:{}",messageQueue,pullStatus);
					break;
				default:break;
				}
			}
		}catch(Exception e){
			logger.error("decide has new message error", e);
		}*/
		return true;
		
	}
}