/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;

import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 1.7.2
 *
 */
public class BrokerRunningTests {

	@Test
	public void testVars() {
		BrokerRunning brokerRunning = BrokerRunning.isBrokerAndManagementRunning();
		brokerRunning.setAdminPassword("foo");
		brokerRunning.setAdminUser("bar");
		brokerRunning.setHostName("baz");
		brokerRunning.setPassword("qux");
		brokerRunning.setPort(1234);
		brokerRunning.setUser("fiz");

		assertEquals("http://baz:15672/api/", brokerRunning.getAdminUri());
		ConnectionFactory connectionFactory = brokerRunning.getConnectionFactory();
		assertEquals("baz", connectionFactory.getHost());
		assertEquals(1234, connectionFactory.getPort());
		assertEquals("fiz", connectionFactory.getUsername());
		assertEquals("qux", connectionFactory.getPassword());
	}

	@Test
	public void testEnvironmentVars() {
		Map<String, String> vars = new HashMap<>();
		vars.put("RABBITMQ_TEST_ADMIN_PASSWORD", "FOO");
		vars.put("RABBITMQ_TEST_ADMIN_URI", "http://foo/bar");
		vars.put("RABBITMQ_TEST_ADMIN_USER", "BAR");
		vars.put("RABBITMQ_TEST_HOSTNAME", "BAZ");
		vars.put("RABBITMQ_TEST_PASSWORD", "QUX");
		vars.put("RABBITMQ_TEST_PORT", "2345");
		vars.put("RABBITMQ_TEST_USER", "FIZ");
		BrokerRunning.setEnvironmentVariableOverrides(vars);
		BrokerRunning brokerRunning = BrokerRunning.isBrokerAndManagementRunning();

		assertEquals("http://foo/bar", brokerRunning.getAdminUri());
		ConnectionFactory connectionFactory = brokerRunning.getConnectionFactory();
		assertEquals("BAZ", connectionFactory.getHost());
		assertEquals(2345, connectionFactory.getPort());
		assertEquals("FIZ", connectionFactory.getUsername());
		assertEquals("QUX", connectionFactory.getPassword());
		DirectFieldAccessor dfa = new DirectFieldAccessor(brokerRunning);
		assertEquals("BAR", dfa.getPropertyValue("adminUser"));
		assertEquals("FOO", dfa.getPropertyValue("adminPassword"));

		BrokerRunning.clearEnvironmentVariableOverrides();
	}

}
