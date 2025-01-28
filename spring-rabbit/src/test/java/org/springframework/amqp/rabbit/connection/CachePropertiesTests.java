/*
 * Copyright 2016-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class CachePropertiesTests {

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
		assertThat(c2).isSameAs(c1);
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
		assertThat(props.getProperty("connectionName")).isEqualTo("testChannelCache");
		assertThat(props.getProperty("channelCacheSize")).isEqualTo("4");
		assertThat(props.getProperty("idleChannelsNotTx")).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsTx")).isEqualTo("3");
		assertThat(props.getProperty("idleChannelsNotTxHighWater")).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsTxHighWater")).isEqualTo("3");
		ch1 = c1.createChannel(false);
		ch3 = c1.createChannel(true);
		props = this.channelCf.getCacheProperties();
		assertThat(props.getProperty("idleChannelsNotTx")).isEqualTo("1");
		assertThat(props.getProperty("idleChannelsTx")).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsNotTxHighWater")).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsTxHighWater")).isEqualTo("3");
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
		assertThat(props.getProperty("idleChannelsNotTx")).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsTx")).isEqualTo("4"); // not 5
		assertThat(props.getProperty("idleChannelsNotTxHighWater")).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsTxHighWater")).isEqualTo("4"); // not 5

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
		assertThat(props.getProperty("channelCacheSize")).isEqualTo("10");
		assertThat(props.getProperty("connectionCacheSize")).isEqualTo("5");
		assertThat(props.getProperty("openConnections")).isEqualTo("2");
		assertThat(props.getProperty("idleConnections")).isEqualTo("1");
		c2.close();
		props = this.connectionCf.getCacheProperties();
		assertThat(props.getProperty("idleConnections")).isEqualTo("2");
		assertThat(props.getProperty("idleConnectionsHighWater")).isEqualTo("2");
		int c1Port = c1.getLocalPort();
		int c2Port = c2.getLocalPort();
		assertThat(props.getProperty("connectionName:" + c1Port)).isEqualTo("testConnectionCache0");
		assertThat(props.getProperty("connectionName:" + c2Port)).isEqualTo("testConnectionCache1");
		assertThat(props.getProperty("idleChannelsNotTx:" + c1Port)).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsTx:" + c1Port)).isEqualTo("0");
		assertThat(props.getProperty("idleChannelsNotTxHighWater:" + c1Port)).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsTxHighWater:" + c1Port)).isEqualTo("0");
		assertThat(props.getProperty("idleChannelsNotTx:" + c2Port)).isEqualTo("1");
		assertThat(props.getProperty("idleChannelsTx:" + c2Port)).isEqualTo("2");
		assertThat(props.getProperty("idleChannelsNotTxHighWater:" + c2Port)).isEqualTo("1");
		assertThat(props.getProperty("idleChannelsTxHighWater:" + c2Port)).isEqualTo("2");
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
