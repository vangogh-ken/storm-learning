package com.tianshouzhi.study.wordcountapp.trident.mq;

import java.util.Date;

import org.apache.commons.lang.time.DateUtils;

import com.alibaba.rocketmq.shade.com.alibaba.fastjson.JSONObject;

/**
 * Trident使用json-simple进行序列化元数据到zookeeper
 * json simple只支持8中基本数据类型+List+Map+JsonAware类型
 * @author TIANSHOUZHI336
 *
 */
@SuppressWarnings("unchecked")
public class RocketMqPartitionMeta extends JSONObject{
	
	/**
	 * 
	 */
	public static final long serialVersionUID = 1003604473740641741L;
	public static final String QUEUE_ID="queueId";
	public static final String CURRENT_OFFSET="currentOffset";
	public static final String NEXT_OFFSET="nextOffset";
	public static final String TRANSACTION_ID="transactionId";
	public static final String OCCUR_TIME="occurTime";
	public static final String BATCH_SIZE= "batchSize";
	public RocketMqPartitionMeta() {
		super();
	}
	
	public RocketMqPartitionMeta(String queueId,String transactionId,long currentOffset,long nextOffset){
		this.setCurrentOffset(currentOffset);
		this.setNextOffset(nextOffset);
		this.setQueueId(queueId);
		this.setTransactionId(transactionId);
	}
 
	public void setQueueId(String queueId){
		this.put(QUEUE_ID, queueId);
	}
	
	public String getQueueId(){
		return (String) this.get(QUEUE_ID);
	}
	
	public void setCurrentOffset(long currentOffset){
		this.put(CURRENT_OFFSET, currentOffset);
	}
	
	public long getCurrentOffset(){
		return (Long) this.get(CURRENT_OFFSET);
	}
	
	public void setNextOffset(long nextOffset){
		this.put(NEXT_OFFSET, nextOffset);
	}
	
	public long getNextOffset(){
		return (Long) this.get(NEXT_OFFSET);
	}
	
	public void setTransactionId(String transactionId){
		this.put(TRANSACTION_ID, transactionId);
	}
	
	public String getTransectionId(){
		return (String) this.get(TRANSACTION_ID);
	}
	
	public void setOccurTime(long occurTime){
		String datetimeStr = new Date(occurTime).toString();
		this.put(OCCUR_TIME,datetimeStr);
	}
	
	public Date getOccurTime(){
		String time=(String) this.get(OCCUR_TIME);
		return DateUtils.parse(time, DateUtils.DEFAULT_FORMAT);
	}
	
	public void setBatchSize(int batchSize){
		this.put(BATCH_SIZE, batchSize);
	}
	public int getBatchSize(){
		return (Integer)this.get(BATCH_SIZE);
	}
	
}
