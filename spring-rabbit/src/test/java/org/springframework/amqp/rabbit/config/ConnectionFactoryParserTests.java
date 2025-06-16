/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.config;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory.AddressShuffleMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public final class ConnectionFactoryParserTests {

	private DefaultListableBeanFactory beanFactory;

	@BeforeEach
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testKitchenSink() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("kitchenSink", CachingConnectionFactory.class);
		assertThat(connectionFactory).isNotNull();
		assertThat(connectionFactory.getChannelCacheSize()).isEqualTo(10);
		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		assertThat(dfa.getPropertyValue("executorService")).isNull();
		assertThat(dfa.getPropertyValue("confirmType")).isEqualTo(ConfirmType.CORRELATED);
		assertThat(dfa.getPropertyValue("publisherReturns")).isEqualTo(Boolean.TRUE);
		assertThat(TestUtils.getPropertyValue(connectionFactory, "rabbitConnectionFactory.requestedHeartbeat")).isEqualTo(123);
		assertThat(TestUtils.getPropertyValue(connectionFactory, "rabbitConnectionFactory.connectionTimeout")).isEqualTo(789);
		assertThat(connectionFactory.getCacheMode()).isEqualTo(CachingConnectionFactory.CacheMode.CHANNEL);
		assertThat(TestUtils.getPropertyValue(connectionFactory, "channelCheckoutTimeout")).isEqualTo(234L);
		assertThat(TestUtils.getPropertyValue(connectionFactory, "connectionLimit")).isEqualTo(456);
		assertThat(TestUtils.getPropertyValue(connectionFactory, "connectionNameStrategy")).isSameAs(beanFactory.getBean(ConnectionNameStrategy.class));
	}

	@Test
	public void testNative() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("native", CachingConnectionFactory.class);
		assertThat(connectionFactory).isNotNull();
		assertThat(connectionFactory.getChannelCacheSize()).isEqualTo(10);
	}

	@Test
	public void testWithExecutor() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("withExecutor", CachingConnectionFactory.class);
		assertThat(connectionFactory).isNotNull();
		Object executor = new DirectFieldAccessor(connectionFactory).getPropertyValue("executorService");
		assertThat(executor).isNotNull();
		ThreadPoolTaskExecutor exec = beanFactory.getBean("exec", ThreadPoolTaskExecutor.class);
		assertThat(executor).isSameAs(exec.getThreadPoolExecutor());
		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		assertThat(dfa.getPropertyValue("confirmType")).isEqualTo(ConfirmType.NONE);
		assertThat(dfa.getPropertyValue("publisherReturns")).isEqualTo(Boolean.FALSE);
		assertThat(connectionFactory.getCacheMode()).isEqualTo(CachingConnectionFactory.CacheMode.CONNECTION);
		assertThat(TestUtils.getPropertyValue(connectionFactory, "rabbitConnectionFactory.connectionTimeout")).isEqualTo(new ConnectionFactory().getConnectionTimeout());
		assertThat(connectionFactory.getConnectionCacheSize()).isEqualTo(10);
	}

	@Test
	public void testWithExecutorService() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("withExecutorService", CachingConnectionFactory.class);
		assertThat(connectionFactory).isNotNull();
		assertThat(connectionFactory.getChannelCacheSize()).isEqualTo(10);
		Object executor = new DirectFieldAccessor(connectionFactory).getPropertyValue("executorService");
		assertThat(executor).isNotNull();
		ExecutorService exec = beanFactory.getBean("execService", ExecutorService.class);
		assertThat(executor).isSameAs(exec);
		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		assertThat(dfa.getPropertyValue("confirmType")).isEqualTo(ConfirmType.SIMPLE);
	}

	@Test
	public void testMultiHost() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("multiHost", CachingConnectionFactory.class);
		assertThat(connectionFactory).isNotNull();
		assertThat(connectionFactory.getChannelCacheSize()).isEqualTo(10);
		DirectFieldAccessor dfa =  new DirectFieldAccessor(connectionFactory);
		@SuppressWarnings("unchecked")
		List<Address> addresses = (List<Address>) dfa.getPropertyValue("addresses");
		assertThat(addresses).hasSize(3);
		assertThat(addresses.get(0).getHost()).isEqualTo("host1");
		assertThat(addresses.get(0).getPort()).isEqualTo(1234);
		assertThat(addresses.get(1).getHost()).isEqualTo("host2");
		assertThat(addresses.get(1).getPort()).isEqualTo(-1);
		assertThat(addresses.get(2).getHost()).isEqualTo("host3");
		assertThat(addresses.get(2).getPort()).isEqualTo(4567);
		assertThat(dfa.getPropertyValue("addressShuffleMode")).isEqualTo(AddressShuffleMode.INORDER);
		assertThat(TestUtils.getPropertyValue(connectionFactory,
				"rabbitConnectionFactory.threadFactory")).isSameAs(beanFactory.getBean("tf"));
	}

	@Test
	void testResolver() {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("resolved", CachingConnectionFactory.class);
		assertThat(TestUtils.getPropertyValue(connectionFactory, "addressResolver"))
				.isSameAs(this.beanFactory.getBean("resolver"));
	}
}
