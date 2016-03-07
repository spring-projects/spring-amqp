/*
 * Copyright 2016 the original author or authors.
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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.test.BrokerRunning;
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

	@Test
	public void testChannelCache() throws Exception {
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
		assertEquals("10", props.getProperty("channelCacheSize"));
		assertEquals("2", props.getProperty("idleChannelsNotTx"));
		assertEquals("3", props.getProperty("idleChannelsTx"));
	}

	@Test
	public void testConnectionCache() throws Exception {
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
	}

	@Configuration
	// @EnableMBeanExport
	public static class Config {

		@Bean
		public CachingConnectionFactory channelCf() {
			CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory("localhost");
			cachingConnectionFactory.setChannelCacheSize(10);
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
