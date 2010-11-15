package org.springframework.amqp.rabbit.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class RabbitTransactionManagerIntegrationTests {

	private static final String ROUTE = "test.queue";

	private RabbitTemplate template;

	private TransactionTemplate transactionTemplate;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueue(ROUTE);

	@Before
	public void init() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		template = new RabbitTemplate(connectionFactory);
		template.setChannelTransacted(true);
		RabbitTransactionManager transactionManager = new RabbitTransactionManager(connectionFactory);
		transactionTemplate = new TransactionTemplate(transactionManager);
	}

	@Test
	public void testSendAndReceiveInTransaction() throws Exception {
		String result = transactionTemplate.execute(new TransactionCallback<String>() {
			public String doInTransaction(TransactionStatus status) {
				template.convertAndSend(ROUTE, "message");
				return (String) template.receiveAndConvert(ROUTE);
			}
		});
		assertEquals(null, result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
	}

	@Test
	public void testReceiveInTransaction() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = transactionTemplate.execute(new TransactionCallback<String>() {
			public String doInTransaction(TransactionStatus status) {
				return (String) template.receiveAndConvert(ROUTE);
			}
		});
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInTransactionWithRollback() throws Exception {
		template.setChannelTransacted(true); // Makes receive (and send in
												// principle) transactional
		template.convertAndSend(ROUTE, "message");
		try {
			transactionTemplate.execute(new TransactionCallback<String>() {
				public String doInTransaction(TransactionStatus status) {
					template.receiveAndConvert(ROUTE);
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		} catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInTransaction() throws Exception {
		template.setChannelTransacted(true);
		transactionTemplate.execute(new TransactionCallback<Void>() {
			public Void doInTransaction(TransactionStatus status) {
				template.convertAndSend(ROUTE, "message");
				return null;
			}
		});
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInTransactionWithRollback() throws Exception {
		template.setChannelTransacted(true);
		try {
			transactionTemplate.execute(new TransactionCallback<Void>() {
				public Void doInTransaction(TransactionStatus status) {
					template.convertAndSend(ROUTE, "message");
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		} catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@SuppressWarnings("serial")
	private class PlannedException extends RuntimeException {
		public PlannedException() {
			super("Planned");
		}
	}

}
