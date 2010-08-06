/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import javax.print.attribute.standard.Destination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.support.RabbitUtils;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MethodInvoker;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;

/**
 * Message listener adapter that delegates the handling of messages to target
 * listener methods via reflection, with flexible message type conversion.
 * Allows listener methods to operate on message content types, completely
 * independent from the Rabbit API.
 *
 * <p>By default, the content of incoming Rabbit messages gets extracted before
 * being passed into the target listener method, to let the target method
 * operate on message content types such as String or byte array instead of
 * the raw {@link Message}. Message type conversion is delegated to a Spring
 * AMQ {@link MessageConverter}. By default, a {@link SimpleMessageConverter}
 * will be used. (If you do not want such automatic message conversion taking
 * place, then be sure to set the {@link #setMessageConverter MessageConverter}
 * to <code>null</code>.)
 *
 * <p>If a target listener method returns a non-null object (typically of a
 * message content type such as <code>String</code> or byte array), it will get
 * wrapped in a Rabbit <code>Message</code> and sent to the exchange of the incoming message
 * with the routingKey that comes from the Rabbit ReplyTo property or via 
 * {@link #setResponseRoutingKey(String) specified routingKey}).
 *
 * <p><b>Note:</b> The sending of response messages is only available when
 * using the {@link ChannelAwareMessageListener} entry point (typically through a
 * Spring message listener container). Usage as {@link MessageListener}
 * does <i>not</i> support the generation of response messages.
 *
 * <p>Find below some examples of method signatures compliant with this
 * adapter class. This first example handles all <code>Message</code> types
 * and gets passed the contents of each <code>Message</code> type as an
 * argument. No <code>Message</code> will be sent back as all of these
 * methods return <code>void</code>.
 *
 * <pre class="code">public interface MessageContentsDelegate {
 *    void handleMessage(String text);
 *    void handleMessage(Map map);
 *    void handleMessage(byte[] bytes);
 *    void handleMessage(Serializable obj);
 * }</pre>
 *
 * This next example handle a <code>Message</code> type and gets
 * passed the actual (raw) <code>Message</code> as an argument. Again, no
 * <code>Message</code> will be sent back as all of these methods return
 * <code>void</code>.
 *
 * <pre class="code">public interface RawMessageDelegate {
 *    void handleMessage(Message message);
 * }</pre>
 *
 * This next example illustrates a <code>Message</code> delegate
 * that just consumes the <code>String</code> contents of
 * {@link Message Messages}. Notice also how the
 * name of the <code>Message</code> handling method is different from the
 * {@link #ORIGINAL_DEFAULT_LISTENER_METHOD original} (this will have to
 * be configured in the attandant bean definition). Again, no <code>Message</code>
 * will be sent back as the method returns <code>void</code>.
 *
 * <pre class="code">public interface TextMessageContentDelegate {
 *    void onMessage(String text);
 * }</pre>
 *
 * This final example illustrates a <code>Message</code> delegate
 * that just consumes the <code>String</code> contents of
 * {@link Message Messages}. Notice how the return type
 * of this method is <code>String</code>: This will result in the configured
 * {@link MessageListenerAdapter} sending a {@link Message} in response.
 *
 * <pre class="code">public interface ResponsiveTextMessageContentDelegate {
 *    String handleMessage(String text);
 * }</pre>
 *
 * For further examples and discussion please do refer to the Spring
 * reference documentation which describes this class (and its attendant
 * XML configuration) in detail.
 *
 * @author Juergen Hoeller
 * @author Mark Pollack
 * @author Mark Fisher
 * @see #setDelegate
 * @see #setDefaultListenerMethod
 * @see #setResponseRoutingKey(String)
 * @see #setMessageConverter
 * @see org.springframework.amqp.support.converter.SimpleMessageConverter
 * @see org.springframework.amqp.rabbit.core.ChannelAwareMessageListener
 * @see org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer#setMessageListener
 */
public class MessageListenerAdapter	implements MessageListener, ChannelAwareMessageListener {

	/**
	 * Out-of-the-box value for the default listener method: "handleMessage".
	 */
	public static final String ORIGINAL_DEFAULT_LISTENER_METHOD = "handleMessage";


	private static final String DEFAULT_RESPONSE_ROUTING_KEY = "";


	// TODO configure defaults
	// void basicQos(int prefetchSize, int prefetchCount, boolean global)
	
	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private Object delegate;

	private String defaultListenerMethod = ORIGINAL_DEFAULT_LISTENER_METHOD;

	private String responseRoutingKey = DEFAULT_RESPONSE_ROUTING_KEY;

	private String responseExchange = null;

	private volatile boolean mandatoryPublish;

	private volatile boolean immediatePublish;

	private MessageConverter messageConverter;


	/**
	 * Create a new {@link MessageListenerAdapter} with default settings.
	 */
	public MessageListenerAdapter() {
		initDefaultStrategies();
		this.delegate = this;
	}

	/**
	 * Create a new {@link MessageListenerAdapter} for the given delegate.
	 * @param delegate the delegate object
	 */
	public MessageListenerAdapter(Object delegate) {
		initDefaultStrategies();
		setDelegate(delegate);
	}
	
	/**
	 * Create a new {@link MessageListenerAdapter} for the given delegate.
	 * @param delegate the delegate object
	 * @param messageConverter the message converter to use
	 */
	public MessageListenerAdapter(Object delegate, MessageConverter messageConverter) {
		initDefaultStrategies();
		setDelegate(delegate);
		setMessageConverter(messageConverter);
	}


	/**
	 * Set a target object to delegate message listening to.
	 * Specified listener methods have to be present on this target object.
	 * <p>If no explicit delegate object has been specified, listener
	 * methods are expected to present on this adapter instance, that is,
	 * on a custom subclass of this adapter, defining listener methods.
	 */
	public void setDelegate(Object delegate) {
		Assert.notNull(delegate, "Delegate must not be null");
		this.delegate = delegate;
	}

	/**
	 * Return the target object to delegate message listening to.
	 */
	protected Object getDelegate() {
		return this.delegate;
	}

	/**
	 * Specify the name of the default listener method to delegate to,
	 * for the case where no specific listener method has been determined.
	 * Out-of-the-box value is {@link #ORIGINAL_DEFAULT_LISTENER_METHOD "handleMessage"}.
	 * @see #getListenerMethodName
	 */
	public void setDefaultListenerMethod(String defaultListenerMethod) {
		this.defaultListenerMethod = defaultListenerMethod;
	}

	/**
	 * Return the name of the default listener method to delegate to.
	 */
	protected String getDefaultListenerMethod() {
		return this.defaultListenerMethod;
	}

	/**
	 * Set the routing key to use when sending response messages. This will be applied
	 * in case of a request message that does not carry a "ReplyTo" property
	 * <p>Response destinations are only relevant for listener methods that return
	 * result objects, which will be wrapped in a response message and sent to a
	 * response destination.
	 */
	public void setResponseRoutingKey(String responseRoutingKey) {
		this.responseRoutingKey = responseRoutingKey;
	}
	
	/**
	 * Set the exchange to use when sending response messages. This is only
	 * used if the exchange from the received message is null.
	 * <p>Response destinations are only relevant for listener methods that return
	 * result objects, which will be wrapped in a response message and sent to a
	 * response destination.
	 * @param responseExchange
	 */
	public void setResponseExchange(String responseExchange) {
		this.responseExchange = responseExchange;
	}

	/**
	 * Set the converter that will convert incoming Rabbit messages to
	 * listener method arguments, and objects returned from listener
	 * methods back to Rabbit messages.
	 * <p>The default converter is a {@link SimpleMessageConverter}, which is able
	 * to handle "text" content-types.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the converter that will convert incoming Rabbit messages to
	 * listener method arguments, and objects returned from listener
	 * methods back to Rabbit messages.
	 */
	protected MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	public void setMandatoryPublish(boolean mandatoryPublish) {
		this.mandatoryPublish = mandatoryPublish;
	}

	public void setImmediatePublish(boolean immediatePublish) {
		this.immediatePublish = immediatePublish;
	}

	/**
	 * Rabbit {@link MessageListener} entry point.
	 * <p>Delegates the message to the target listener method, with appropriate
	 * conversion of the message argument. In case of an exception, the
	 * {@link #handleListenerException(Throwable)} method will be invoked.
	 * <p><b>Note:</b> Does not support sending response messages based on
	 * result objects returned from listener methods. Use the
	 * {@link ChannelAwareMessageListener} entry point (typically through a Spring
	 * message listener container) for handling result objects as well.
	 * @param message the incoming Rabbit message
	 * @see #handleListenerException
	 * @see #onMessage(Message, Channel)
	 */
	public void onMessage(Message message) {
		try {
			onMessage(message, null);
		}
		catch (Throwable ex) {
			handleListenerException(ex);
		}
	}

	/**
	 * Spring {@link org.springframework.jms.listener.SessionAwareMessageListener}
	 * entry point.
	 * <p>Delegates the message to the target listener method, with appropriate
	 * conversion of the message argument. If the target method returns a
	 * non-null object, wrap in a Rabbit message and send it back.
	 * @param message the incoming Rabbit message
	 * @param channel the Rabbit channel to operate on
	 * @throws Exception if thrown by Rabbit API methods
	 */
	public void onMessage(Message message, Channel channel) throws Exception {
		// Check whether the delegate is a MessageListener impl itself.
		// In that case, the adapter will simply act as a pass-through.
		Object delegate = getDelegate();
		if (delegate != this) {
			if (delegate instanceof ChannelAwareMessageListener) {
				if (channel != null) {
					((ChannelAwareMessageListener) delegate).onMessage(message, channel);
					return;
				}
				else if (!(delegate instanceof MessageListener)) {
					throw new AmqpIllegalStateException("MessageListenerAdapter cannot handle a " +
							"ChannelAwareMessageListener delegate if it hasn't been invoked with a Channel itself");
				}
			}
			if (delegate instanceof MessageListener) {
				((MessageListener) delegate).onMessage(message);
				return;
			}
		}

		// Regular case: find a handler method reflectively.
		Object convertedMessage = extractMessage(message);
		String methodName = getListenerMethodName(message, convertedMessage);
		if (methodName == null) {
			throw new AmqpIllegalStateException("No default listener method specified: " +
					"Either specify a non-null value for the 'defaultListenerMethod' property or " +
					"override the 'getListenerMethodName' method.");
		}

		// Invoke the handler method with appropriate arguments.
		Object[] listenerArguments = buildListenerArguments(convertedMessage);
		Object result = invokeListenerMethod(methodName, listenerArguments);
		if (result != null) {
			handleResult(result, message, channel);
		}
		else {
			logger.trace("No result object given - no result to handle");
		}
	}

	/**
	 * Initialize the default implementations for the adapter's strategies.
	 * @see #setMessageConverter
	 * @see org.springframework.amqp.support.converter.SimpleMessageConverter
	 */
	protected void initDefaultStrategies() {
		setMessageConverter(new SimpleMessageConverter());
	}

    /**
	 * Handle the given exception that arose during listener execution.
	 * The default implementation logs the exception at error level.
	 * <p>This method only applies when using a Rabbit {@link MessageListener}.
	 * In case of the Spring
	 * {@link org.springframework.jms.listener.SessionAwareMessageListener}
	 * mechanism, exceptions get handled by the caller instead.
	 * @param ex the exception to handle
	 * @see #onMessage(Message)
	 */
	protected void handleListenerException(Throwable ex) {
		logger.error("Listener execution failed", ex);
	}

	/**
	 * Extract the message body from the given Rabbit message.
	 * @param message the Rabbit <code>Message</code>
	 * @return the content of the message, to be passed into the
	 * listener method as argument
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected Object extractMessage(Message message) throws Exception {
		MessageConverter converter = getMessageConverter();
		if (converter != null) {
			return converter.fromMessage(message);
		}
		return message;
	}

	/**
	 * Determine the name of the listener method that is supposed to
	 * handle the given message.
	 * <p>The default implementation simply returns the configured
	 * default listener method, if any.
	 * @param originalMessage the Rabbit request message
	 * @param extractedMessage the converted Rabbit request message,
	 * to be passed into the listener method as argument
	 * @return the name of the listener method (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #setDefaultListenerMethod
	 */
	protected String getListenerMethodName(Message originalMessage, Object extractedMessage) throws Exception {
		return getDefaultListenerMethod();
	}

	/**
	 * Build an array of arguments to be passed into the target listener method.
	 * Allows for multiple method arguments to be built from a single message object.
	 * <p>The default implementation builds an array with the given message object
	 * as sole element. This means that the extracted message will always be passed
	 * into a <i>single</i> method argument, even if it is an array, with the target
	 * method having a corresponding single argument of the array's type declared.
	 * <p>This can be overridden to treat special message content such as arrays
	 * differently, for example passing in each element of the message array
	 * as distinct method argument.
	 * @param extractedMessage the content of the message
	 * @return the array of arguments to be passed into the
	 * listener method (each element of the array corresponding
	 * to a distinct method argument)
	 */
	protected Object[] buildListenerArguments(Object extractedMessage) {
		return new Object[] {extractedMessage};
	}

	/**
	 * Invoke the specified listener method.
	 * @param methodName the name of the listener method
	 * @param arguments the message arguments to be passed in
	 * @return the result returned from the listener method
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #getListenerMethodName
	 * @see #buildListenerArguments
	 */
	protected Object invokeListenerMethod(String methodName, Object[] arguments) throws Exception {
		try {
			MethodInvoker methodInvoker = new MethodInvoker();
			methodInvoker.setTargetObject(getDelegate());
			methodInvoker.setTargetMethod(methodName);
			methodInvoker.setArguments(arguments);
			methodInvoker.prepare();
			return methodInvoker.invoke();
		}
		catch (InvocationTargetException ex) {
			Throwable targetEx = ex.getTargetException();
			if (targetEx instanceof IOException) {
				throw new AmqpIOException((IOException) targetEx);
			}
			else {
				throw new ListenerExecutionFailedException(
						"Listener method '" + methodName + "' threw exception", targetEx);
			}
		}
		catch (Throwable ex) {
			ArrayList<String> arrayClass = new ArrayList<String>();
			if (arguments != null) {
				for (int i = 0; i < arguments.length; i++) {
					arrayClass.add(arguments[i].getClass().toString());
				}
			}
			throw new ListenerExecutionFailedException("Failed to invoke target method '" + methodName +
					"' with argument type = [" + StringUtils.collectionToCommaDelimitedString(arrayClass) + "], value = [" 
					+ ObjectUtils.nullSafeToString(arguments) + "]", ex);
		}
	}


	/**
	 * Handle the given result object returned from the listener method,
	 * sending a response message back.
	 * @param result the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param channel the Rabbit channel to operate on (may be <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #buildMessage
	 * @see #postProcessResponse
	 * @see #getReplyToAddress(Message)
	 * @see #sendResponse
	 */
	protected void handleResult(Object result, Message request, Channel channel) throws Exception {
		if (channel != null) {
			if (logger.isDebugEnabled()) {
				logger.debug("Listener method returned result [" + result +
						"] - generating response message for it");
			}
			Message response = buildMessage(channel, result);
			postProcessResponse(request, response);
			Address replyTo = getReplyToAddress(request);
			sendResponse(channel, replyTo, response);
		}
		else if (logger.isWarnEnabled()) {
			logger.warn("Listener method returned result [" + result +
					"]: not generating response message for it because of no Rabbit Channel given");
		}
	}

	protected String getReceivedExchange(Message request) {
		return request.getMessageProperties().getReceivedExchange();
	}

	/**
	 * Build a Rabbit message to be sent as response based on the given result object.
	 * @param session the Rabbit Channel to operate on
	 * @param result the content of the message, as returned from the listener method
	 * @return the Rabbit <code>Message</code> (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #setMessageConverter
	 */
	protected Message buildMessage(Channel session, Object result) throws Exception {
		MessageConverter converter = getMessageConverter();
		if (converter != null) {
			return converter.toMessage(result, new RabbitMessageProperties());
		}
		else {
			if (!(result instanceof Message)) {
				throw new MessageConversionException(
						"No MessageConverter specified - cannot handle message [" + result + "]");
			}
			return (Message) result;
		}
	}

	/**
	 * Post-process the given response message before it will be sent.
	 * <p>The default implementation sets the response's correlation id
	 * to the request message's correlation id, if any; otherwise to the
	 * request message id.
	 * @param request the original incoming Rabbit message
	 * @param response the outgoing Rabbit message about to be sent
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected void postProcessResponse(Message request, Message response) throws Exception {
		byte[] correlation = request.getMessageProperties().getCorrelationId();
	
		if (correlation == null) {
			String messageId = request.getMessageProperties().getMessageId();
			if (messageId != null) {
				correlation = messageId.getBytes(SimpleMessageConverter.DEFAULT_CHARSET);
			}
		}
		response.getMessageProperties().setCorrelationId(correlation);
	}

	/**
	 * Determine a reply-to Address for the given message.
	 * <p>The default implementation first checks the Rabbit Reply-To
	 * Address of the supplied request; if that is not <code>null</code>
	 * it is returned; if it is <code>null</code>, then the configured
	 * default response Exchange and routing key are used to construct
	 * a reply-to Address. If the responseExchange property is also
	 * <code>null</code>, then an {@link AmqpException} is thrown.
	 * @param request the original incoming Rabbit message
	 * @return the reply-to Address (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @throws AmqpException if no {@link Address} can be determined
	 * @see #setResponseExchange(String)
	 * @see #setResponseRoutingKey(String)
	 * @see org.springframework.amqp.core.Message#getMessageProperties()
	 * @see org.springframework.amqp.core.MessageProperties#getReplyTo()
	 */
	protected Address getReplyToAddress(Message request) throws Exception {
		Address replyTo = request.getMessageProperties().getReplyTo(); 			
		if (replyTo == null) {
			if (this.responseExchange == null) {
				throw new AmqpException("Cannot determine ReplyTo message property value: " +
						"Request message does not contain reply-to property, and no default response Exchange was set.");
			}
			replyTo = new Address(null, this.responseExchange, this.responseRoutingKey);
		}
		return replyTo;
	}


	/**
	 * Send the given response message to the given destination.
	 * @param channel the Rabbit channel to operate on
	 * @param replyTo the Rabbit ReplyTo string to use when sending.  Currently interpreted to be the routing key.
	 * @param message the Rabbit message to send
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #postProcessResponse(Message, Message)
	 */
	protected void sendResponse(Channel channel, Address replyTo, Message message) throws Exception {
		postProcessChannel(channel, message);
		
		//TODO parameterize out default encoding,  How to ensure there is a valid binding for this publish?
		try {
			logger.debug("Publishing response to exchanage = [" + replyTo.getExchangeName() + "], routingKey = [" + replyTo.getRoutingKey() + "]");			
			channel.basicPublish(replyTo.getExchangeName(), replyTo.getRoutingKey(),
					this.mandatoryPublish, this.immediatePublish,
					RabbitUtils.extractBasicProperties(message, "UTF-8"),
					message.getBody());
		} catch (Exception ex) {
			throw RabbitUtils.convertRabbitAccessException(ex);
		}
	}

	/**
	 * Post-process the given message producer before using it to send the response.
	 * <p>The default implementation is empty.
	 * @param response the outgoing Rabbit message about to be sent
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected void postProcessChannel(Channel channel, Message response) throws Exception {
	}

}
