/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.aopalliance.aop.Advice;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.retry.policy.MapRetryContextCache;
import org.springframework.retry.policy.RetryContextCache;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public class MissingIdRetryTests {

	private volatile CountDownLatch latch;

	@ClassRule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@BeforeClass
	@AfterClass
	public static void setupAndCleanUp() {
		RabbitAdmin admin = brokerIsRunning.getAdmin();
		admin.deleteQueue("retry.test.queue");
		admin.deleteExchange("retry.test.exchange");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testWithNoId() throws Exception {
		// 2 messsages; each retried once by missing id interceptor
		this.latch = new CountDownLatch(4);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new POJO()));
		container.setQueueNames("retry.test.queue");

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();

		// use an external template so we can share his cache
		RetryTemplate retryTemplate = new RetryTemplate();
		RetryContextCache cache = new MapRetryContextCache();
		retryTemplate.setRetryContextCache(cache);
		fb.setRetryOperations(retryTemplate);

		// give him a reference to the retry cache so he can clean it up
		MissingMessageIdAdvice missingIdAdvice = new MissingMessageIdAdvice(cache);

		Advice retryInterceptor = fb.getObject();
		// add both advices
		container.setAdviceChain(new Advice[] {missingIdAdvice, retryInterceptor});
		container.start();

		template.convertAndSend("retry.test.exchange", "retry.test.binding", "Hello, world!");
		template.convertAndSend("retry.test.exchange", "retry.test.binding", "Hello, world!");
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		Thread.sleep(2000);
		assertEquals(0, ((Map) new DirectFieldAccessor(cache).getPropertyValue("map")).size());
		container.stop();
		ctx.close();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testWithId() throws Exception {
		// 2 messsages; each retried twice by retry interceptor
		this.latch = new CountDownLatch(6);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new POJO()));
		container.setQueueNames("retry.test.queue");

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();

		// use an external template so we can share his cache
		RetryTemplate retryTemplate = new RetryTemplate();
		RetryContextCache cache = new MapRetryContextCache();
		retryTemplate.setRetryContextCache(cache);
		fb.setRetryOperations(retryTemplate);
		fb.setMessageRecoverer(new RejectAndDontRequeueRecoverer());

		// give him a reference to the retry cache so he can clean it up
		MissingMessageIdAdvice missingIdAdvice = new MissingMessageIdAdvice(cache);

		Advice retryInterceptor = fb.getObject();
		// add both advices
		container.setAdviceChain(new Advice[] {missingIdAdvice, retryInterceptor});
		container.start();

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("text/plain");
		messageProperties.setMessageId("foo");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		template.send("retry.test.exchange", "retry.test.binding", message);
		template.send("retry.test.exchange", "retry.test.binding", message);
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		Thread.sleep(2000);
		assertEquals(0, ((Map) new DirectFieldAccessor(cache).getPropertyValue("map")).size());
		container.stop();
		ctx.close();
	}

	public class POJO {
		public void handleMessage(String foo) {
			latch.countDown();
			throw new RuntimeException("fail");
		}
	}
}
