package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer;
import org.springframework.amqp.rabbit.listener.BlockingQueueConsumer.Delivery;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

import com.rabbitmq.client.Channel;

public class RabbitBindingIntegrationTests {

	private Queue queue;

	private RabbitTemplate template = new RabbitTemplate(
			new CachingConnectionFactory());

	@Rule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	@Before
	public void declareQueue() {
		RabbitAdmin admin = new RabbitAdmin(template);
		try {
			admin.deleteQueue("test.queue");
		} catch (AmqpIOException e) {
			// Ignore (queue didn't exist)
		}
		queue = new Queue("test.queue");
		// Idempotent, so no problem to do this for every test
		admin.declareQueue(queue);
		admin.purgeQueue("test.queue", false);
	}

	@Test
	public void testSendAndReceiveWithTopicSingleCallback() throws Exception {

		final RabbitAdmin admin = new RabbitAdmin(template);
		final TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.from(queue).to(exchange)
				.with("*.end"));

		template.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = new BlockingQueueConsumer(
						channel);
				String tag = channel.basicConsume(queue.getName(), true,
						consumer);
				assertNotNull(tag);

				template.convertAndSend("foo", "message");
				String result = getResult(consumer);
				assertEquals(null, result);

				template.convertAndSend("foo.end", "message");
				result = getResult(consumer);
				assertEquals("message", result);

				return null;

			}
		});

	}

	@Test
	// @Ignore("Not sure yet if we need to support a use case like this")
	public void testSendAndReceiveWithTopicConsumeInBackground()
			throws Exception {

		RabbitAdmin admin = new RabbitAdmin(template);
		TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.from(queue).to(exchange)
				.with("*.end"));

		final RabbitTemplate template = new RabbitTemplate(
				new CachingConnectionFactory());
		template.setExchange(exchange.getName());

		BlockingQueueConsumer consumer = template
				.execute(new ChannelCallback<BlockingQueueConsumer>() {
					public BlockingQueueConsumer doInRabbit(Channel channel)
							throws Exception {

						BlockingQueueConsumer consumer = new BlockingQueueConsumer(
								channel);
						String tag = channel.basicConsume(queue.getName(),
								true, consumer);
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

	}

	@Test
	public void testSendAndReceiveWithTopicTwoCallbacks() throws Exception {

		RabbitAdmin admin = new RabbitAdmin(template);
		TopicExchange exchange = new TopicExchange("topic");
		admin.declareExchange(exchange);
		template.setExchange(exchange.getName());

		admin.declareBinding(BindingBuilder.from(queue).to(exchange)
				.with("*.end"));

		template.execute(new ChannelCallback<Void>() {
			public Void doInRabbit(Channel channel) throws Exception {

				BlockingQueueConsumer consumer = new BlockingQueueConsumer(
						channel);
				String tag = channel.basicConsume(queue.getName(), true,
						consumer);
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

				BlockingQueueConsumer consumer = new BlockingQueueConsumer(
						channel);
				String tag = channel.basicConsume(queue.getName(), true,
						consumer);
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

	private String getResult(final BlockingQueueConsumer consumer)
			throws InterruptedException {
		Delivery response = consumer.nextDelivery(200L);
		if (response == null) {
			return null;
		}
		MessageProperties messageProps = RabbitUtils.createMessageProperties(
				response.getProperties(), response.getEnvelope(), "UTF-8");
		return (String) new SimpleMessageConverter().fromMessage(new Message(
				response.getBody(), messageProps));
	}
}
