/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.rabbit.stream.micrometer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.rabbitmq.stream.Message;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.transport.ReceiverContext;

import org.springframework.amqp.rabbit.support.micrometer.RabbitListenerObservation;
import org.springframework.amqp.rabbit.support.micrometer.RabbitListenerObservationConvention;
import org.springframework.amqp.rabbit.support.micrometer.RabbitMessageReceiverContext;

/**
 * {@link ReceiverContext} for stream {@link Message}s.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public class RabbitStreamMessageReceiverContext extends ReceiverContext<Message> {

	private final String listenerId;

	private final String stream;

	@SuppressWarnings("this-escape")
	public RabbitStreamMessageReceiverContext(Message message, String listenerId, String stream) {
		super((carrier, key) -> {
			Map<String, Object> props = carrier.getApplicationProperties();
			if (props != null) {
				Object value = carrier.getApplicationProperties().get(key);
				if (value instanceof String string) {
					return string;
				}
				else if (value instanceof byte[] bytes) {
					return new String(bytes, StandardCharsets.UTF_8);
				}
			}
			return null;
		});
		setCarrier(message);
		this.listenerId = listenerId;
		this.stream = stream;
		setRemoteServiceName("RabbitMQ Stream");
	}

	public String getListenerId() {
		return this.listenerId;
	}

	/**
	 * Return the source (stream) for this message.
	 * @return the source.
	 */
	public String getSource() {
		return this.stream;
	}

	/**
	 * Default {@link RabbitListenerObservationConvention} for Rabbit listener key values.
	 */
	public static class DefaultRabbitListenerObservationConvention implements RabbitListenerObservationConvention {

		/**
		 * A singleton instance of the convention.
		 */
		public static final DefaultRabbitListenerObservationConvention INSTANCE =
				new DefaultRabbitListenerObservationConvention();

		@Override
		public KeyValues getLowCardinalityKeyValues(RabbitMessageReceiverContext context) {
			return KeyValues.of(RabbitListenerObservation.ListenerLowCardinalityTags.LISTENER_ID.asString(),
							context.getListenerId());
		}

		@Override
		public String getContextualName(RabbitMessageReceiverContext context) {
			return context.getSource() + " receive";
		}

	}

}
