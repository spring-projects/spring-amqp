/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.rabbitmq.client.Channel;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class CachePropertiesTests {

	@ClassRule
	public static BrokerRunning br = BrokerRunning.isRunning();

	@Autowired
	private CachingConnectionFactory channelCf;

	@Autowired
	private CachingConnectionFactory connectionCf;

	private final AtomicInteger connNumber = new AtomicInteger();

	@Test
	public void testChannelCache() throws Exception {
		this.channelCf.setConnectionNameStrategy(cf -> "testChannelCache");
		Connection c1 = this.channelCf.createConnection();
		Connection c2 = this.channelCf.createConnection();
		assertSame(c1, c2);
		Channel ch1 = c1.createChannel(false);
		Channel ch2 = c1.createChannel(false);
		Channel ch3 = c1.createChannel(true);
		Channel ch4 = c1.createChannel(true);
		Channel ch5 = c1.createChannel(true);
		ch1.close();
		ch2.close();
		ch3.close();
		ch4.close();
		ch5.close();
		Properties props = this.channelCf.getCacheProperties();
		assertEquals("testChannelCache", props.getProperty("connectionName"));
		assertEquals("4", props.getProperty("channelCacheSize"));
		assertEquals("2", props.getProperty("idleChannelsNotTx"));
		assertEquals("3", props.getProperty("idleChannelsTx"));
		assertEquals("2", props.getProperty("idleChannelsNotTxHighWater"));
		assertEquals("3", props.getProperty("idleChannelsTxHighWater"));
		ch1 = c1.createChannel(false);
		ch3 = c1.createChannel(true);
		props = this.channelCf.getCacheProperties();
		assertEquals("1", props.getProperty("idleChannelsNotTx"));
		assertEquals("2", props.getProperty("idleChannelsTx"));
		assertEquals("2", props.getProperty("idleChannelsNotTxHighWater"));
		assertEquals("3", props.getProperty("idleChannelsTxHighWater"));
		ch1 = c1.createChannel(false);
		ch2 = c1.createChannel(false);
		ch3 = c1.createChannel(true);
		ch4 = c1.createChannel(true);
		ch5 = c1.createChannel(true);
		Channel ch6 = c1.createChannel(true);
		Channel ch7 = c1.createChannel(true); // #5
		ch1.close();
		ch2.close();
		ch3.close();
		ch4.close();
		ch5.close();
		ch6.close();
		ch7.close();
		props = this.channelCf.getCacheProperties();
		assertEquals("2", props.getProperty("idleChannelsNotTx"));
		assertEquals("4", props.getProperty("idleChannelsTx")); // not 5
		assertEquals("2", props.getProperty("idleChannelsNotTxHighWater"));
		assertEquals("4", props.getProperty("idleChannelsTxHighWater")); // not 5

	}

	@Test
	public void testConnectionCache() throws Exception {
		this.connectionCf.setConnectionNameStrategy(cf -> "testConnectionCache" + this.connNumber.getAndIncrement());
		Connection c1 = this.connectionCf.createConnection();
		Connection c2 = this.connectionCf.createConnection();
		Channel ch1 = c1.createChannel(false);
		Channel ch2 = c1.createChannel(false);
		Channel ch3 = c2.createChannel(true);
		Channel ch4 = c2.createChannel(true);
		Channel ch5 = c2.createChannel(false);
		ch1.close();
		ch2.close();
		ch3.close();
		ch4.close();
		ch5.close();
		c1.close();
		Properties props = this.connectionCf.getCacheProperties();
		assertEquals("10", props.getProperty("channelCacheSize"));
		assertEquals("5", props.getProperty("connectionCacheSize"));
		assertEquals("2", props.getProperty("openConnections"));
		assertEquals("1", props.getProperty("idleConnections"));
		c2.close();
		props = this.connectionCf.getCacheProperties();
		assertEquals("2", props.getProperty("idleConnections"));
		assertEquals("2", props.getProperty("idleConnectionsHighWater"));
		int c1Port = c1.getLocalPort();
		int c2Port = c2.getLocalPort();
		assertEquals("testConnectionCache0", props.getProperty("connectionName:" + c1Port));
		assertEquals("testConnectionCache1", props.getProperty("connectionName:" + c2Port));
		assertEquals("2", props.getProperty("idleChannelsNotTx:" + c1Port));
		assertEquals("0", props.getProperty("idleChannelsTx:" + c1Port));
		assertEquals("2", props.getProperty("idleChannelsNotTxHighWater:" + c1Port));
		assertEquals("0", props.getProperty("idleChannelsTxHighWater:" + c1Port));
		assertEquals("1", props.getProperty("idleChannelsNotTx:" + c2Port));
		assertEquals("2", props.getProperty("idleChannelsTx:" + c2Port));
		assertEquals("1", props.getProperty("idleChannelsNotTxHighWater:" + c2Port));
		assertEquals("2", props.getProperty("idleChannelsTxHighWater:" + c2Port));
	}

	@Configuration
	// @EnableMBeanExport
	public static class Config {

		@Bean
		public CachingConnectionFactory channelCf() {
			CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory("localhost");
			cachingConnectionFactory.setChannelCacheSize(4);
			return cachingConnectionFactory;
		}

		@Bean
		public CachingConnectionFactory connectionCf() {
			CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory("localhost");
			cachingConnectionFactory.setChannelCacheSize(10);
			cachingConnectionFactory.setConnectionCacheSize(5);
			cachingConnectionFactory.setCacheMode(CacheMode.CONNECTION);
			return cachingConnectionFactory;
		}

	}

}
