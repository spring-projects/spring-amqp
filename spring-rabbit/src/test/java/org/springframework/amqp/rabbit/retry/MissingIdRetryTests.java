/*
 * Copyright 2002-2017 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.StatefulRetryOperationsInterceptorFactoryBean;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.MapRetryContextCache;
import org.springframework.retry.policy.RetryContextCache;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Gary Russell
 * @author Arnaud Cogoluègnes
 * @since 1.1.2
 *
 */
public class MissingIdRetryTests {

	private final Log logger = LogFactory.getLog(MissingIdRetryTests.class);

	private volatile CountDownLatch latch;

	@ClassRule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@Rule
	public LogLevelAdjuster adjuster = new LogLevelAdjuster(Level.DEBUG, BlockingQueueConsumer.class,
			MissingIdRetryTests.class,
			RetryTemplate.class, SimpleRetryPolicy.class);

	@BeforeClass
	@AfterClass
	public static void setupAndCleanUp() {
		brokerIsRunning.deleteQueues("retry.test.queue");
		brokerIsRunning.deleteExchanges("retry.test.exchange");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testWithNoId() throws Exception {
		// 2 messages; each retried once by missing id interceptor
		this.latch = new CountDownLatch(4);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setStatefulRetryFatalWithNullMessageId(false);
		container.setMessageListener(new MessageListenerAdapter(new POJO()));
		container.setQueueNames("retry.test.queue");

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();

		// use an external template so we can share his cache
		RetryTemplate retryTemplate = new RetryTemplate();
		RetryContextCache cache = spy(new MapRetryContextCache());
		retryTemplate.setRetryContextCache(cache);
		fb.setRetryOperations(retryTemplate);

		Advice retryInterceptor = fb.getObject();
		container.setAdviceChain(retryInterceptor);
		container.start();

		template.convertAndSend("retry.test.exchange", "retry.test.binding", "Hello, world!");
		template.convertAndSend("retry.test.exchange", "retry.test.binding", "Hello, world!");
		try {
			assertTrue(latch.await(30, TimeUnit.SECONDS));
			Map map = (Map) new DirectFieldAccessor(cache).getPropertyValue("map");
			int n = 0;
			while (n++ < 100 && map.size() != 0) {
				Thread.sleep(100);
			}
			verify(cache, never()).put(any(), any(RetryContext.class));
			verify(cache, never()).remove(any());
			assertEquals("Expected map.size() = 0, was: " + map.size(), 0, map.size());
		}
		finally {
			container.stop();
			ctx.close();
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testWithId() throws Exception {
		// 2 messages; each retried twice by retry interceptor
		this.latch = new CountDownLatch(6);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setPrefetchCount(1);
		container.setMessageListener(new MessageListenerAdapter(new POJO()));
		container.setQueueNames("retry.test.queue");

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();

		// use an external template so we can share his cache
		RetryTemplate retryTemplate = new RetryTemplate();
		RetryContextCache cache = spy(new MapRetryContextCache());
		retryTemplate.setRetryContextCache(cache);
		fb.setRetryOperations(retryTemplate);
		fb.setMessageRecoverer(new RejectAndDontRequeueRecoverer());

		Advice retryInterceptor = fb.getObject();
		container.setAdviceChain(retryInterceptor);
		container.start();

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("text/plain");
		messageProperties.setMessageId("foo");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		template.send("retry.test.exchange", "retry.test.binding", message);
		template.send("retry.test.exchange", "retry.test.binding", message);
		try {
			assertTrue(latch.await(30, TimeUnit.SECONDS));
			Map map = (Map) new DirectFieldAccessor(cache).getPropertyValue("map");
			int n = 0;
			while (n++ < 100 && map.size() != 0) {
				Thread.sleep(100);
			}
			ArgumentCaptor putCaptor = ArgumentCaptor.forClass(Object.class);
			ArgumentCaptor getCaptor = ArgumentCaptor.forClass(Object.class);
			ArgumentCaptor removeCaptor = ArgumentCaptor.forClass(Object.class);
			verify(cache, times(6)).put(putCaptor.capture(), any(RetryContext.class));
			verify(cache, times(6)).get(getCaptor.capture());
			verify(cache, atLeast(2)).remove(removeCaptor.capture());
			verify(cache, atMost(4)).remove(removeCaptor.capture());
			logger.debug("puts:" + putCaptor.getAllValues());
			logger.debug("gets:" + putCaptor.getAllValues());
			logger.debug("removes:" + removeCaptor.getAllValues());
			assertEquals("Expected map.size() = 0, was: " + map.size(), 0, map.size());
		}
		finally {
			container.stop();
			ctx.close();
		}
	}

	@SuppressWarnings({ "rawtypes", "resource" })
	@Test
	public void testWithIdAndSuccess() throws Exception {
		// 2 messages; each retried twice by retry interceptor
		this.latch = new CountDownLatch(6);
		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("retry-context.xml", this.getClass());
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		final Set<String> processed = new HashSet<>();
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(m -> {
			latch.countDown();
			if (!processed.contains(m.getMessageProperties().getMessageId())) {
				processed.add(m.getMessageProperties().getMessageId());
				throw new RuntimeException("fail");
			}
		});
		container.setQueueNames("retry.test.queue");

		StatefulRetryOperationsInterceptorFactoryBean fb = new StatefulRetryOperationsInterceptorFactoryBean();

		// use an external template so we can share his cache
		RetryTemplate retryTemplate = new RetryTemplate();
		RetryContextCache cache = spy(new MapRetryContextCache());
		retryTemplate.setRetryContextCache(cache);
		fb.setRetryOperations(retryTemplate);
		fb.setMessageRecoverer(new RejectAndDontRequeueRecoverer());

		Advice retryInterceptor = fb.getObject();
		container.setAdviceChain(retryInterceptor);
		container.start();

		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("text/plain");
		messageProperties.setMessageId("foo");
		Message message = new Message("Hello, world!".getBytes(), messageProperties);
		template.send("retry.test.exchange", "retry.test.binding", message);
		messageProperties.setMessageId("bar");
		template.send("retry.test.exchange", "retry.test.binding", message);
		try {
			assertTrue(latch.await(30, TimeUnit.SECONDS));
			Map map = (Map) new DirectFieldAccessor(cache).getPropertyValue("map");
			int n = 0;
			while (n++ < 100 && map.size() != 0) {
				Thread.sleep(100);
			}
			ArgumentCaptor putCaptor = ArgumentCaptor.forClass(Object.class);
			ArgumentCaptor getCaptor = ArgumentCaptor.forClass(Object.class);
			ArgumentCaptor removeCaptor = ArgumentCaptor.forClass(Object.class);
			verify(cache, times(2)).put(putCaptor.capture(), any(RetryContext.class));
			verify(cache, times(2)).get(getCaptor.capture());
			verify(cache, atLeast(2)).remove(removeCaptor.capture());
			verify(cache, atMost(4)).remove(removeCaptor.capture());
			logger.debug("puts:" + putCaptor.getAllValues());
			logger.debug("gets:" + putCaptor.getAllValues());
			logger.debug("removes:" + removeCaptor.getAllValues());
			assertEquals("Expected map.size() = 0, was: " + map.size(), 0, map.size());
		}
		finally {
			container.stop();
			ctx.close();
		}
	}

	public class POJO {

		public void handleMessage(String foo) {
			latch.countDown();
			throw new RuntimeException("fail");
		}

	}

}
