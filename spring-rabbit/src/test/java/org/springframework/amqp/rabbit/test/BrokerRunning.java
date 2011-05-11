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
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.util.StringUtils;

/**
 * <p>
 * A rule that prevents integration tests from failing if the Rabbit broker application is not running or not
 * accessible. If the Rabbit broker is not running in the background all the tests here will simply be skipped because
 * of a violated assumption (showing as successful). Usage:
 * </p>
 * 
 * <pre>
 * &#064;Rule
 * public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();
 * 
 * &#064;Test
 * public void testSendAndReceive() throws Exception {
 * 	// ... test using RabbitTemplate etc.
 * }
 * </pre>
 * <p>
 * The rule can be declared as static so that it only has to check once for all tests in the enclosing test case, but
 * there isn't a lot of overhead in making it non-static.
 * </p>
 * 
 * @see Assume
 * @see AssumptionViolatedException
 * 
 * @author Dave Syer
 * 
 */
public class BrokerRunning extends TestWatchman {

	private static final String DEFAULT_QUEUE_NAME = BrokerRunning.class.getName();

	private static Log logger = LogFactory.getLog(BrokerRunning.class);

	// Static so that we only test once on failure: speeds up test suite
	private static Map<Integer,Boolean> brokerOnline = new HashMap<Integer, Boolean>();

	// Static so that we only test once on failure
	private static Map<Integer,Boolean> brokerOffline = new HashMap<Integer, Boolean>();

	private final boolean assumeOnline;

	private final boolean purge;

	private Queue[] queues;
	
	private int DEFAULT_PORT = BrokerTestUtils.getPort();

	private int port;

	private String hostName = null;

	/**
	 * Ensure the broker is running and has an empty queue with the specified name in the default exchange.
	 * 
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunningWithEmptyQueues(String... names) {
		Queue[] queues = new Queue[names.length];
		for (int i = 0; i < queues.length; i++) {
			queues[i] = new Queue(names[i]);
		}
		return new BrokerRunning(true, true, queues);
	}

	/**
	 * Ensure the broker is running and has an empty queue (which can be addressed via the default exchange).
	 * 
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunningWithEmptyQueues(Queue... queues) {
		return new BrokerRunning(true, true, queues);
	}

	/**
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunning() {
		return new BrokerRunning(true);
	}

	/**
	 * @return a new rule that assumes there is no existing broker
	 */
	public static BrokerRunning isNotRunning() {
		return new BrokerRunning(false);
	}

	private BrokerRunning(boolean assumeOnline, boolean purge, Queue... queues) {
		this.assumeOnline = assumeOnline;
		this.queues = queues;
		this.purge = purge;
		setPort(DEFAULT_PORT);
	}

	private BrokerRunning(boolean assumeOnline, Queue... queues) {
		this(assumeOnline, false, queues);
	}

	private BrokerRunning(boolean assumeOnline) {
		this(assumeOnline, new Queue(DEFAULT_QUEUE_NAME));
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

			for (Queue queue : queues) {
				String queueName = queue.getName();

				if (purge) {
					logger.debug("Deleting queue: " + queueName);
					// Delete completely - gets rid of consumers and bindings as well
					admin.deleteQueue(queueName);
				}

				if (isDefaultQueue(queueName)) {
					// Just for test probe.
					admin.deleteQueue(queueName);
				} else {
					admin.declareQueue(queue);
				}
			}
			brokerOffline.put(port, false);
			if (!assumeOnline) {
				Assume.assumeTrue(brokerOffline.get(port));
			}

		} catch (Exception e) {
			logger.warn("Not executing tests because basic connectivity test failed", e);
			brokerOnline.put(port, false);
			if (assumeOnline) {
				Assume.assumeNoException(e);
			}
		} finally {
			connectionFactory.destroy();
		}

		return super.apply(base, method, target);

	}

	private boolean isDefaultQueue(String queue) {
		return DEFAULT_QUEUE_NAME.equals(queue);
	}

}
