/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.amqp.rabbit.admin;


/**
 * This class represents a Queue that is configured on the RabbitMQ broker
 * 
 * @author Mark Pollack
 *
 */
public class QueueInfo {
	/*{app.stock.request={transactions=0, acks_uncommitted=0, 
	 * 					  consumers=0, pid=#Pid<rabbit@MARK6500.150.0>,
	 *                    durable=false, messages=0, memory=2320, auto_delete=false, 
	 *                    messages_ready=0, arguments=[], name=app.stock.request, 
	 *                   messages_unacknowledged=0, messages_uncommitted=0}, 
	 */

	private long transactions;
	
	private long acksUncommitted;
	
	private long consumers;
	
	private String pid;
	
	private boolean durable;
	
	private long messages;
	
	private long memory;
	
	private boolean autoDelete;
	
	private long messagesReady;
	
	private String[] arguments;
	
	private String name;
	
	private long messagesUnacknowledged;
	
	private long messageUncommitted;

	public long getTransactions() {
		return transactions;
	}

	public void setTransactions(long transations) {
		this.transactions = transations;
	}

	public long getAcksUncommitted() {
		return acksUncommitted;
	}

	public void setAcksUncommitted(long acksUncommitted) {
		this.acksUncommitted = acksUncommitted;
	}

	public long getConsumers() {
		return consumers;
	}

	public void setConsumers(long consumers) {
		this.consumers = consumers;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public boolean isDurable() {
		return durable;
	}

	public void setDurable(boolean durable) {
		this.durable = durable;
	}

	public long getMessages() {
		return messages;
	}

	public void setMessages(long messages) {
		this.messages = messages;
	}

	public long getMemory() {
		return memory;
	}

	public void setMemory(long memory) {
		this.memory = memory;
	}

	public boolean isAutoDelete() {
		return autoDelete;
	}

	public void setAutoDelete(boolean autoDelete) {
		this.autoDelete = autoDelete;
	}

	public long getMessagesReady() {
		return messagesReady;
	}

	public void setMessagesReady(long messagesReady) {
		this.messagesReady = messagesReady;
	}

	public String[] getArguments() {
		return arguments;
	}

	public void setArguments(String[] args) {
		this.arguments = args;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getMessagesUnacknowledged() {
		return messagesUnacknowledged;
	}

	public void setMessagesUnacknowledged(long messagesUnacknowledged) {
		this.messagesUnacknowledged = messagesUnacknowledged;
	}

	public long getMessageUncommitted() {
		return messageUncommitted;
	}

	public void setMessageUncommitted(long messageUncommitted) {
		this.messageUncommitted = messageUncommitted;
	}

	@Override
	public String toString() {
		return "QueueInfo [name=" + name + ", durable=" + durable
				+ ", autoDelete=" + autoDelete + ", arguments=" + arguments
				+ ", memory=" + memory + ", messages=" + messages
				+ ", consumers=" + consumers + ", transations=" + transactions
				+ ", acksUncommitted=" + acksUncommitted + ", messagesReady="
				+ messagesReady + ", messageUncommitted=" + messageUncommitted
				+ ", messagesUnacknowledged=" + messagesUnacknowledged
				+ ", pid=" + pid + "]";
	}
	
	
}
