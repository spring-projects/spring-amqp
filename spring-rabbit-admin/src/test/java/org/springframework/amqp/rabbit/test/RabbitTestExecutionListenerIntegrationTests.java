package org.springframework.amqp.rabbit.test;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.test.context.ContextConfiguration;

@RunWith(SpringRabbitJUnit4ClassRunner.class)
@ContextConfiguration
@RabbitConfiguration
@Ignore // only works on Windows at the moment
public class RabbitTestExecutionListenerIntegrationTests {

	@Test
	public void doNothing() throws InterruptedException {
		Thread.sleep(1000);
		System.out.println("inside DO NOTHING");
		System.out.println("inside DO NOTHING");
	}

	@Test
	public void doNothinAgain() throws InterruptedException {
		Thread.sleep(1000);
		System.out.println("inside DO AGAIN");
		System.out.println("inside DO AGAIN");
	}
}
