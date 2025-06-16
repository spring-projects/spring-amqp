/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;

import static org.assertj.core.api.Assertions.assertThat;

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

		assertThat(brokerRunning.getAdminUri()).isEqualTo("http://baz:15672/api/");
		ConnectionFactory connectionFactory = brokerRunning.getConnectionFactory();
		assertThat(connectionFactory.getHost()).isEqualTo("baz");
		assertThat(connectionFactory.getPort()).isEqualTo(1234);
		assertThat(connectionFactory.getUsername()).isEqualTo("fiz");
		assertThat(connectionFactory.getPassword()).isEqualTo("qux");
	}

	@Test
	public void testEnvironmentVars() {
		Map<String, String> vars = new HashMap<>();
		vars.put("RABBITMQ_TEST_ADMIN_PASSWORD", "FOO");
		vars.put("RABBITMQ_TEST_ADMIN_URI", "https://foo/bar");
		vars.put("RABBITMQ_TEST_ADMIN_USER", "BAR");
		vars.put("RABBITMQ_TEST_HOSTNAME", "BAZ");
		vars.put("RABBITMQ_TEST_PASSWORD", "QUX");
		vars.put("RABBITMQ_TEST_PORT", "2345");
		vars.put("RABBITMQ_TEST_USER", "FIZ");
		BrokerRunning.setEnvironmentVariableOverrides(vars);
		BrokerRunning brokerRunning = BrokerRunning.isBrokerAndManagementRunning();

		assertThat(brokerRunning.getAdminUri()).isEqualTo("https://foo/bar");
		ConnectionFactory connectionFactory = brokerRunning.getConnectionFactory();
		assertThat(connectionFactory.getHost()).isEqualTo("BAZ");
		assertThat(connectionFactory.getPort()).isEqualTo(2345);
		assertThat(connectionFactory.getUsername()).isEqualTo("FIZ");
		assertThat(connectionFactory.getPassword()).isEqualTo("QUX");
		DirectFieldAccessor dfa = new DirectFieldAccessor(brokerRunning);
		assertThat(dfa.getPropertyValue("brokerRunning.adminUser")).isEqualTo("BAR");
		assertThat(dfa.getPropertyValue("brokerRunning.adminPassword")).isEqualTo("FOO");

		BrokerRunning.clearEnvironmentVariableOverrides();
	}

}
