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
 * It is recommended to declare the rule as static so that it only has to check once for all tests in the enclosing test
 * case.
 * </p>
 * 
 * @see Assume
 * @see AssumptionViolatedException
 * 
 * @author Dave Syer
 * 
 */
public class BrokerRunning extends TestWatchman {

	private static Log logger = LogFactory.getLog(BrokerRunning.class);

	private boolean brokerOnline = true;

	private boolean brokerOffline = true;

	private final boolean assumeOnline;

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

	private BrokerRunning(boolean assumeOnline) {
		this.assumeOnline = assumeOnline;
	}

	@Override
	public Statement apply(Statement base, FrameworkMethod method, Object target) {

		if (assumeOnline) {
			Assume.assumeTrue(brokerOnline);
		} else {
			Assume.assumeTrue(brokerOffline);
		}

		try {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			RabbitAdmin admin = new RabbitAdmin(connectionFactory);
			admin.declareQueue(new Queue("test.broker.running"));
			admin.deleteQueue("test.broker.running");
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

}
