package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.listener.ActiveObjectCounter;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

import com.rabbitmq.client.Channel;

public class RabbitBindingIntegrationTests {

	private static Queue queue = new Queue("test.queue");

	private ConnectionFactory connectionFactory = new CachingConnectionFactory();

	private RabbitTemplate template = new RabbitTemplate(connectionFactory );

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue);

	@Test
	public void testSendAndReceiveWithTopicSingleCallback() throws Exception {

		final RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		final TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		template.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = createConsumer(template);
				String tag = consumer.getConsumerTag();
				assertNotNull(tag);

				template.convertAndSend("foo", "message");

				try {

					String result = getResult(consumer);
					assertEquals(null, result);

					template.convertAndSend("foo.end", "message");
					result = getResult(consumer);
					assertEquals("message", result);

				} finally {
					channel.basicCancel(tag);
				}

				return null;

			}

		});

	}

	@Test
	public void testSendAndReceiveWithNonDefaultExchange() throws Exception {

		final RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		final TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		template.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = createConsumer(template);
				String tag = consumer.getConsumerTag();
				assertNotNull(tag);

				template.convertAndSend("topic", "foo", "message");

				try {

					String result = getResult(consumer);
					assertEquals(null, result);

					template.convertAndSend("topic", "foo.end", "message");
					result = getResult(consumer);
					assertEquals("message", result);

				} finally {
					channel.basicCancel(tag);
				}

				return null;

			}
		});

	}

	@Test
	// @Ignore("Not sure yet if we need to support a use case like this")
	public void testSendAndReceiveWithTopicConsumeInBackground() throws Exception {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		final RabbitTemplate template = new RabbitTemplate(new CachingConnectionFactory());
		template.setExchange(exchange.getName());

		BlockingQueueConsumer consumer = template.execute(new ChannelCallback<BlockingQueueConsumer>() {
			public BlockingQueueConsumer doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = createConsumer(template);
				String tag = consumer.getConsumerTag();
				assertNotNull(tag);

				return consumer;

			}
		});

		template.convertAndSend("foo", "message");
		String result = getResult(consumer);
		assertEquals(null, result);

		template.convertAndSend("foo.end", "message");
		result = getResult(consumer);
		assertEquals("message", result);

		consumer.getChannel().basicCancel(consumer.getConsumerTag());

	}

	@Test
	public void testSendAndReceiveWithTopicTwoCallbacks() throws Exception {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("*.end"));

		template.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = createConsumer(template);
				String tag = consumer.getConsumerTag();
				assertNotNull(tag);

				try {
					template.convertAndSend("foo", "message");
					String result = getResult(consumer);
					assertEquals(null, result);
				} finally {
					channel.basicCancel(tag);
				}

				return null;

			}
		});

		template.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = createConsumer(template);
				String tag = consumer.getConsumerTag();
				assertNotNull(tag);

				try {
					template.convertAndSend("foo.end", "message");
					String result = getResult(consumer);
					assertEquals("message", result);
				} finally {
					channel.basicCancel(tag);
				}

				return null;

			}
		});

	}

	@Test
	public void testSendAndReceiveWithFanout() throws Exception {

		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		FanoutExchange exchange = new FanoutExchange("fanout");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.bind(queue).to(exchange));

		template.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = createConsumer(template);
				String tag = consumer.getConsumerTag();
				assertNotNull(tag);

				try {
					template.convertAndSend("message");
					String result = getResult(consumer);
					assertEquals("message", result);
				} finally {
					channel.basicCancel(tag);
				}

				return null;

			}
		});

	}

	private BlockingQueueConsumer createConsumer(RabbitAccessor accessor) {
		BlockingQueueConsumer consumer = new BlockingQueueConsumer(accessor.getConnectionFactory(), new ActiveObjectCounter<BlockingQueueConsumer>(), AcknowledgeMode.AUTO, true, 1, queue.getName());
		consumer.start();
		return consumer;
	}

	private String getResult(final BlockingQueueConsumer consumer) throws InterruptedException {
		Message response = consumer.nextMessage(200L);
		if (response == null) {
			return null;
		}
		return (String) new SimpleMessageConverter().fromMessage(response);
	}
}
