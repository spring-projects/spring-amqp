package org.springframework.amqp.rabbit.admin;

import java.util.concurrent.atomic.AtomicInteger;

public class PojoHandler {

	private final AtomicInteger messageCount = new AtomicInteger();

	public void handleMessage(String textMessage) {
		int msgCount = this.messageCount.incrementAndGet();
		System.out.println("Thread [" + Thread.currentThread().getId() + "]  PojoHandler Received Message " + msgCount + ", = " + textMessage);			
	}

}
