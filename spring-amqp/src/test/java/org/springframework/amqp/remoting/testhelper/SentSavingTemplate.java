package org.springframework.amqp.remoting.testhelper;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;

public class SentSavingTemplate extends AbstractAmqpTemplate {
	private Message lastMessage = null;
	private String lastExchange = null;
	private String lastRoutingKey = null;

	@Override
	public void send(String exchange, String routingKey, Message message) throws AmqpException {
		this.lastExchange = exchange;
		this.lastRoutingKey = routingKey;
		this.lastMessage = message;
	}

	public Message getLastMessage() {
		return lastMessage;
	}

	public String getLastExchange() {
		return lastExchange;
	}

	public String getLastRoutingKey() {
		return lastRoutingKey;
	}
}
