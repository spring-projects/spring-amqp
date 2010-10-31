package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

public class RabbitTemplateIntegrationTests {

	private static final String ROUTE = "test.queue";

	private RabbitTemplate template = new RabbitTemplate(new CachingConnectionFactory());

	@Rule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@Before
	public void declareQueue() {
		RabbitAdmin admin = new RabbitAdmin(template);
		try {
			admin.deleteQueue(ROUTE);
		}
		catch (AmqpIOException e) {
			// Ignore (queue didn't exist)
		}
		admin.declareQueue(new Queue(ROUTE));
		admin.purgeQueue(ROUTE, false);
	}

	@Test
	public void testSendAndReceive() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveTransacted() throws Exception {
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveTransactedWithUncachedConnection() throws Exception {
		RabbitTemplate template = new RabbitTemplate(new SingleConnectionFactory());
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		// TODO: If channel is transacted with a SingleConnectionFactory the
		// message is always rolled back to the broker
		// after receive (correct but surprising for users)
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveTransactedWithImplicitRollback() throws Exception {
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		// Rollback of manual receive is implicit because the channel is
		// closed...
		try {
			template.execute(new ChannelCallback<String>() {
				public String doInRabbit(Channel channel) throws Exception {
					// Switch off the auto-ack so the message is rolled back...
					channel.basicGet(ROUTE, false);
					// This is the way to rollback with a cached channel (it is
					// the way the ConnectionFactoryUtils
					// handles it via a synchronization):
					channel.basicRecover(true);
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		}
		catch (UncategorizedAmqpException e) {
			// TODO: allow client exception to propagate if no AMQP related
			assertTrue(e.getCause() instanceof PlannedException);
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendAndReceiveInCallback() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = template.execute(new ChannelCallback<String>() {
			public String doInRabbit(Channel channel) throws Exception {
				// We need noAck=false here for the message to be expicitly
				// acked
				GetResponse response = channel.basicGet(ROUTE, false);
				MessageProperties messageProps = RabbitUtils.createMessageProperties(response.getProps(),
						response.getEnvelope(), "UTF-8");
				// Explicit ack
				channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
				return (String) new SimpleMessageConverter().fromMessage(new Message(response.getBody(), messageProps));
			}
		});
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInExternalTransaction() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = new TransactionTemplate(new TestTransactionManager())
				.execute(new TransactionCallback<String>() {
					public String doInTransaction(TransactionStatus status) {
						return (String) template.receiveAndConvert(ROUTE);
					}
				});
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInExternalTransactionWithRollback() throws Exception {
		template.setChannelTransacted(true); // Makes receive (and send in
												// principle) transactional
		template.convertAndSend(ROUTE, "message");
		try {
			new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallback<String>() {
				public String doInTransaction(TransactionStatus status) {
					template.receiveAndConvert(ROUTE);
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		}
		catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInExternalTransaction() throws Exception {
		template.setChannelTransacted(true);
		new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallback<Void>() {
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
	public void testSendInExternalTransactionWithRollback() throws Exception {
		template.setChannelTransacted(true);
		try {
			new TransactionTemplate(new TestTransactionManager()).execute(new TransactionCallback<Void>() {
				public Void doInTransaction(TransactionStatus status) {
					template.convertAndSend(ROUTE, "message");
					throw new PlannedException();
				}
			});
			fail("Expected PlannedException");
		}
		catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testAtomicSendAndReceive() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// Set up a consumer to respond to out producer
		Future<String> received = executor.submit(new Callable<String>() {

			public String call() throws Exception {
				Message message = null;
				for (int i = 0; i < 10; i++) {
					// TODO: AMQP-71 add receive timeout
					message = template.receive(ROUTE);
					if (message != null) {
						break;
					}
					Thread.sleep(100L);
				}
				assertNotNull("No message received", message);
				template.send(message.getMessageProperties().getReplyTo().getRoutingKey(), message);
				return (String) template.getMessageConverter().fromMessage(message);
			}

		});
		String result = (String) template.convertSendAndReceive(ROUTE, "message");
		assertEquals("message", received.get(1000, TimeUnit.MILLISECONDS));
		assertEquals("message", result);
		// Message was consumed so nothing left on queue
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@SuppressWarnings("serial")
	private class PlannedException extends RuntimeException {
		public PlannedException() {
			super("Planned");
		}
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		}

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}

}
