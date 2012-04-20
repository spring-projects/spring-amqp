/*
 * Copyright 2002-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assume;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestWatchman;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.springframework.amqp.core.FederatedExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.util.StringUtils;

/**
 * <p>
 * A rule that prevents integration tests from failing if the Rabbit broker application is not running or not
 * accessible, with test federation configuration:
 * <pre>
 * [
 *    {rabbitmq_federation,
 *     [
 *       {upstream_sets, [{"upstream-set", [[{connection, "upstream-server"} ]]}
 *                       ]},
 *       {connections, [{"upstream-server", [{host, "somehost"}]}
 *                     ]}
 *     ]
 *    }
 * ]
 * </pre>
 * </p>
 *
 * @See BrokerRunning
 * @see Assume
 * @see AssumptionViolatedException
 *
 * @author Gary Russell
 *
 */
public class BrokerFederated extends TestWatchman {

	private static Log logger = LogFactory.getLog(BrokerFederated.class);

	// Static so that we only test once on failure: speeds up test suite
	private static Map<Integer,Boolean> brokerOnline = new HashMap<Integer, Boolean>();

	// Static so that we only test once on failure
	private static Map<Integer,Boolean> brokerOffline = new HashMap<Integer, Boolean>();

	private final boolean assumeOnline;

	private int DEFAULT_PORT = BrokerTestUtils.getPort();

	private int port;

	private String hostName = null;

	/**
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerFederated isRunning() {
		return new BrokerFederated();
	}

	private BrokerFederated() {
		this.assumeOnline = true;
		setPort(DEFAULT_PORT);
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
		if (!brokerOffline.containsKey(port)) {
			brokerOffline.put(port, true);
		}
		if (!brokerOnline.containsKey(port)) {
			brokerOnline.put(port, true);
		}
	}

	/**
	 * @param hostName the hostName to set
	 */
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	@Override
	public Statement apply(Statement base, FrameworkMethod method, Object target) {

		// Check at the beginning, so this can be used as a static field
		if (assumeOnline) {
			Assume.assumeTrue(brokerOnline.get(port));
		} else {
			Assume.assumeTrue(brokerOffline.get(port));
		}

		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();

		try {

			connectionFactory.setPort(port);
			if (StringUtils.hasText(hostName)) {
				connectionFactory.setHost(hostName);
			}
			RabbitAdmin admin = new RabbitAdmin(connectionFactory);
			FederatedExchange exchange = new FederatedExchange("fedDirectRuleTest");
			exchange.setBackingType("direct");
			exchange.setUpstreamSet("upstream-set");
			admin.declareExchange(exchange);
			admin.deleteExchange("fedDirectRuleTest");

			brokerOffline.put(port, false);

			if (!assumeOnline) {
				Assume.assumeTrue(brokerOffline.get(port));
			}

		} catch (Exception e) {
			logger.warn("Not executing tests because federated connectivity test failed", e);
			brokerOnline.put(port, false);
			if (assumeOnline) {
				Assume.assumeNoException(e);
			}
		} finally {
			connectionFactory.destroy();
		}

		return super.apply(base, method, target);

	}

}
