/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.logback;

import java.util.concurrent.CountDownLatch;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author Jon Brisbin
 * @author Gary Russell
 */
public class TestListener implements MessageListener {

	private final CountDownLatch latch;

	private volatile Message message;

	public TestListener(int count) {
		latch = new CountDownLatch(count);
	}

	public CountDownLatch getLatch() {
		return latch;
	}

	public Message getMessage() {
		return message;
	}

	public Object getId() {
		if (this.message == null || this.getMessageProperties() == null) {
			throw new IllegalStateException("No MessageProperties received");
		}
		return this.message.getMessageProperties().getMessageId();
	}

	public MessageProperties getMessageProperties() {
		if (this.message == null) {
			throw new IllegalStateException("No Message received");
		}
		return this.message.getMessageProperties();
	}

	@Override
	public void onMessage(Message message) {
		System .out .println("MESSAGE: " + message);
		System .out .println("BODY: " + new String(message.getBody()));
		this.message = message;
		latch.countDown();
	}

}
