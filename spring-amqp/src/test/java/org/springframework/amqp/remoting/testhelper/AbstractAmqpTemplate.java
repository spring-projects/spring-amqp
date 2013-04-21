package org.springframework.amqp.remoting.testhelper;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;

public abstract class AbstractAmqpTemplate implements AmqpTemplate {

	@Override
	public void send(Message message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void send(String routingKey, Message message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void send(String exchange, String routingKey, Message message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void convertAndSend(Object message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void convertAndSend(String routingKey, Object message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void convertAndSend(String exchange, String routingKey, Object message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void convertAndSend(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void convertAndSend(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public void convertAndSend(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Message receive() throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Message receive(String queueName) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object receiveAndConvert() throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object receiveAndConvert(String queueName) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Message sendAndReceive(Message message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Message sendAndReceive(String routingKey, Message message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Message sendAndReceive(String exchange, String routingKey, Message message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object convertSendAndReceive(Object message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object convertSendAndReceive(String routingKey, Object message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object convertSendAndReceive(String exchange, String routingKey, Object message) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object convertSendAndReceive(Object message, MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object convertSendAndReceive(String routingKey, Object message, MessagePostProcessor messagePostProcessor)
			throws AmqpException {
		throw new NotImplementedException();
	}

	@Override
	public Object convertSendAndReceive(String exchange, String routingKey, Object message,
			MessagePostProcessor messagePostProcessor) throws AmqpException {
		throw new NotImplementedException();
	}

}
