package org.springframework.amqp.rabbit.test;

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

	private boolean brokerOnline = true;

	private boolean brokerOffline = true;

	private final boolean assumeOnline;

	private final boolean purge;

	private Queue queue;

	/**
	 * Ensure the broker is running and has an empty queue with the specified name in the default exchange.
	 * 
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunningWithEmptyQueue(String queue) {
		return new BrokerRunning(true, new Queue(queue), true);
	}

	/**
	 * Ensure the broker is running and has an empty queue in the default exchange.
	 * 
	 * @return a new rule that assumes an existing running broker
	 */
	public static BrokerRunning isRunningWithEmptyQueue(Queue queue) {
		return new BrokerRunning(true, queue, true);
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

	private BrokerRunning(boolean assumeOnline, Queue queue, boolean purge) {
		this.assumeOnline = assumeOnline;
		this.queue = queue;
		this.purge = purge;
	}

	private BrokerRunning(boolean assumeOnline, Queue queue) {
		this(assumeOnline, queue, false);
	}

	private BrokerRunning(boolean assumeOnline) {
		this(assumeOnline, new Queue(DEFAULT_QUEUE_NAME));
	}

	@Override
	public Statement apply(Statement base, FrameworkMethod method, Object target) {

		// Check at the beginning, so this can be used as a static field
		if (assumeOnline) {
			Assume.assumeTrue(brokerOnline);
		} else {
			Assume.assumeTrue(brokerOffline);
		}

		try {

			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			RabbitAdmin admin = new RabbitAdmin(connectionFactory);

			String queueName = queue.getName();

			if (purge) {
				logger.debug("Deleting queue: " + queueName);
				// Delete completely - gets rid of consumers and bindings as well
				admin.deleteQueue(queueName);
			}

			admin.declareQueue(queue);

			if (isDefaultQueue(queueName)) {
				// Just for test probe.
				admin.deleteQueue(queueName);
				queue = null;
			}
			brokerOffline = false;
			if (!assumeOnline) {
				Assume.assumeTrue(brokerOffline);
			}

		} catch (Exception e) {
			logger.warn("Not executing tests because basic connectivity test failed", e);
			brokerOnline = false;
			if (assumeOnline) {
				Assume.assumeNoException(e);
			}
		}

		return super.apply(base, method, target);

	}

	private boolean isDefaultQueue(String queue) {
		return DEFAULT_QUEUE_NAME.equals(queue);
	}

}
