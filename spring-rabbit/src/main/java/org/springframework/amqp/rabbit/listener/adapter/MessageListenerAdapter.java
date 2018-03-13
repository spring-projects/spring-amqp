/*
 * Copyright 2002-2018 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MethodInvoker;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;

/**
 * Message listener adapter that delegates the handling of messages to target listener methods via reflection, with
 * flexible message type conversion. Allows listener methods to operate on message content types, completely independent
 * from the Rabbit API.
 *
 * <p>
 * By default, the content of incoming Rabbit messages gets extracted before being passed into the target listener
 * method, to let the target method operate on message content types such as String or byte array instead of the raw
 * {@link Message}. Message type conversion is delegated to a Spring AMQ {@link MessageConverter}. By default, a
 * {@link SimpleMessageConverter} will be used. (If you do not want such automatic message conversion taking place, then
 * be sure to set the {@link #setMessageConverter MessageConverter} to <code>null</code>.)
 *
 * <p>
 * If a target listener method returns a non-null object (typically of a message content type such as
 * <code>String</code> or byte array), it will get wrapped in a Rabbit <code>Message</code> and sent to the exchange of
 * the incoming message with the routingKey that comes from the Rabbit ReplyTo property or via
 * {@link #setResponseRoutingKey(String) specified routingKey}).
 *
 * <p>
 * <b>Note:</b> The sending of response messages is only available when using the {@link ChannelAwareMessageListener}
 * entry point (typically through a Spring message listener container). Usage as {@link MessageListener} does <i>not</i>
 * support the generation of response messages.
 *
 * <p>
 * Find below some examples of method signatures compliant with this adapter class. This first example handles all
 * <code>Message</code> types and gets passed the contents of each <code>Message</code> type as an argument. No
 * <code>Message</code> will be sent back as all of these methods return <code>void</code>.
 *
 * <pre class="code">
 * public interface MessageContentsDelegate {
 * 	void handleMessage(String text);
 *
 * 	void handleMessage(Map map);
 *
 * 	void handleMessage(byte[] bytes);
 *
 * 	void handleMessage(Serializable obj);
 * }
 * </pre>
 *
 * This next example handle a <code>Message</code> type and gets passed the actual (raw) <code>Message</code> as an
 * argument. Again, no <code>Message</code> will be sent back as all of these methods return <code>void</code>.
 *
 * <pre class="code">
 * public interface RawMessageDelegate {
 * 	void handleMessage(Message message);
 * }
 * </pre>
 *
 * This next example illustrates a <code>Message</code> delegate that just consumes the <code>String</code> contents of
 * {@link Message Messages}. Notice also how the name of the <code>Message</code> handling method is different from the
 * {@link #ORIGINAL_DEFAULT_LISTENER_METHOD original} (this will have to be configured in the attandant bean
 * definition). Again, no <code>Message</code> will be sent back as the method returns <code>void</code>.
 *
 * <pre class="code">
 * public interface TextMessageContentDelegate {
 * 	void onMessage(String text);
 * }
 * </pre>
 *
 * This final example illustrates a <code>Message</code> delegate that just consumes the <code>String</code> contents of
 * {@link Message Messages}. Notice how the return type of this method is <code>String</code>: This will result in the
 * configured {@link MessageListenerAdapter} sending a {@link Message} in response.
 *
 * <pre class="code">
 * public interface ResponsiveTextMessageContentDelegate {
 * 	String handleMessage(String text);
 * }
 * </pre>
 *
 * For further examples and discussion please do refer to the Spring reference documentation which describes this class
 * (and its attendant XML configuration) in detail.
 *
 * @author Juergen Hoeller
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Gary Russell
 * @author Greg Turnquist
 *
 * @see #setDelegate
 * @see #setDefaultListenerMethod
 * @see #setResponseRoutingKey(String)
 * @see #setMessageConverter
 * @see org.springframework.amqp.support.converter.SimpleMessageConverter
 * @see org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener
 * @see org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer#setMessageListener
 */
public class MessageListenerAdapter extends AbstractAdaptableMessageListener {

	private final Map<String, String> queueOrTagToMethodName = new HashMap<String, String>();

	/**
	 * Out-of-the-box value for the default listener method: "handleMessage".
	 */
	public static final String ORIGINAL_DEFAULT_LISTENER_METHOD = "handleMessage";


	private Object delegate;

	private String defaultListenerMethod = ORIGINAL_DEFAULT_LISTENER_METHOD;


	/**
	 * Create a new {@link MessageListenerAdapter} with default settings.
	 */
	public MessageListenerAdapter() {
		this.delegate = this;
	}

	/**
	 * Create a new {@link MessageListenerAdapter} for the given delegate.
	 * @param delegate the delegate object
	 */
	public MessageListenerAdapter(Object delegate) {
		doSetDelegate(delegate);
	}

	/**
	 * Create a new {@link MessageListenerAdapter} for the given delegate.
	 * @param delegate the delegate object
	 * @param messageConverter the message converter to use
	 */
	public MessageListenerAdapter(Object delegate, MessageConverter messageConverter) {
		doSetDelegate(delegate);
		super.setMessageConverter(messageConverter);
	}

	/**
	 * Create a new {@link MessageListenerAdapter} for the given delegate while also
	 * declaring its POJO method.
	 * @param delegate the delegate object
	 * @param defaultListenerMethod name of the POJO method to call upon message receipt
	 */
	public MessageListenerAdapter(Object delegate, String defaultListenerMethod) {
		this(delegate);
		this.defaultListenerMethod = defaultListenerMethod;
	}

	/**
	 * Set a target object to delegate message listening to. Specified listener methods have to be present on this
	 * target object.
	 * <p> If no explicit delegate object has been specified, listener methods are expected to present on this adapter
	 * instance, that is, on a custom subclass of this adapter, defining listener methods.
	 * @param delegate The delegate listener or POJO.
	 */
	public void setDelegate(Object delegate) {
		doSetDelegate(delegate);
	}

	private void doSetDelegate(Object delegate) {
		Assert.notNull(delegate, "Delegate must not be null");
		this.delegate = delegate;
	}

	/**
	 * @return The target object to delegate message listening to.
	 */
	protected Object getDelegate() {
		return this.delegate;
	}

	/**
	 * Specify the name of the default listener method to delegate to, for the case where no specific listener method
	 * has been determined. Out-of-the-box value is {@link #ORIGINAL_DEFAULT_LISTENER_METHOD "handleMessage"}.
	 * @param defaultListenerMethod The name of the default listener method.
	 * @see #getListenerMethodName
	 */
	public void setDefaultListenerMethod(String defaultListenerMethod) {
		this.defaultListenerMethod = defaultListenerMethod;
	}

	/**
	 * @return The name of the default listener method to delegate to.
	 */
	protected String getDefaultListenerMethod() {
		return this.defaultListenerMethod;
	}


	/**
	 * Set the mapping of queue name or consumer tag to method name. The first lookup
	 * is by queue name, if that returns null, we lookup by consumer tag, if that
	 * returns null, the {@link #setDefaultListenerMethod(String) defaultListenerMethod}
	 * is used.
	 * @param queueOrTagToMethodName the map.
	 * @since 1.5
	 */
	public void setQueueOrTagToMethodName(Map<String, String> queueOrTagToMethodName) {
		this.queueOrTagToMethodName.putAll(queueOrTagToMethodName);
	}

	/**
	 * Add the mapping of a queue name or consumer tag to a method name. The first lookup
	 * is by queue name, if that returns null, we lookup by consumer tag, if that
	 * returns null, the {@link #setDefaultListenerMethod(String) defaultListenerMethod}
	 * is used.
	 * @param queueOrTag The queue name or consumer tag.
	 * @param methodName The method name.
	 * @since 1.5
	 */
	public void addQueueOrTagToMethodName(String queueOrTag, String methodName) {
		this.queueOrTagToMethodName.put(queueOrTag, methodName);
	}

	/**
	 * Remove the mapping of a queue name or consumer tag to a method name.
	 * @param queueOrTag The queue name or consumer tag.
	 * @return the method name that was removed, or null.
	 * @since 1.5
	 */
	public String removeQueueOrTagToMethodName(String queueOrTag) {
		return this.queueOrTagToMethodName.remove(queueOrTag);
	}

	/**
	 * Spring {@link ChannelAwareMessageListener} entry point.
	 * <p>
	 * Delegates the message to the target listener method, with appropriate conversion of the message argument. If the
	 * target method returns a non-null object, wrap in a Rabbit message and send it back.
	 * @param message the incoming Rabbit message
	 * @param channel the Rabbit channel to operate on
	 * @throws Exception if thrown by Rabbit API methods
	 */
	@Override
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
					throw new AmqpIllegalStateException("MessageListenerAdapter cannot handle a "
							+ "ChannelAwareMessageListener delegate if it hasn't been invoked with a Channel itself");
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
			throw new AmqpIllegalStateException("No default listener method specified: "
					+ "Either specify a non-null value for the 'defaultListenerMethod' property or "
					+ "override the 'getListenerMethodName' method.");
		}

		// Invoke the handler method with appropriate arguments.
		Object[] listenerArguments = buildListenerArguments(convertedMessage);
		Object result = invokeListenerMethod(methodName, listenerArguments, message);
		if (result != null) {
			handleResult(result, message, channel);
		}
		else {
			logger.trace("No result object given - no result to handle");
		}
	}

	/**
	 * Determine the name of the listener method that will handle the given message.
	 * <p>
	 * The default implementation first consults the
	 * {@link #setQueueOrTagToMethodName(Map) queueOrTagToMethodName} map looking for a
	 * match on the consumer queue or consumer tag; if no match found, it simply returns
	 * the configured default listener method, or "handleMessage" if not configured.
	 * @param originalMessage the Rabbit request message
	 * @param extractedMessage the converted Rabbit request message, to be passed into the
	 * listener method as argument
	 * @return the name of the listener method (never <code>null</code>)
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #setDefaultListenerMethod
	 * @see #setQueueOrTagToMethodName
	 */
	protected String getListenerMethodName(Message originalMessage, Object extractedMessage) throws Exception {
		if (this.queueOrTagToMethodName.size() > 0) {
			MessageProperties props = originalMessage.getMessageProperties();
			String methodName = this.queueOrTagToMethodName.get(props.getConsumerQueue());
			if (methodName == null) {
				methodName = this.queueOrTagToMethodName.get(props.getConsumerTag());
			}
			if (methodName != null) {
				return methodName;
			}
		}
		return getDefaultListenerMethod();
	}

	/**
	 * Build an array of arguments to be passed into the target listener method. Allows for multiple method arguments to
	 * be built from a single message object.
	 * <p>
	 * The default implementation builds an array with the given message object as sole element. This means that the
	 * extracted message will always be passed into a <i>single</i> method argument, even if it is an array, with the
	 * target method having a corresponding single argument of the array's type declared.
	 * <p>
	 * This can be overridden to treat special message content such as arrays differently, for example passing in each
	 * element of the message array as distinct method argument.
	 * @param extractedMessage the content of the message
	 * @return the array of arguments to be passed into the listener method (each element of the array corresponding to
	 * a distinct method argument)
	 */
	protected Object[] buildListenerArguments(Object extractedMessage) {
		return new Object[] {extractedMessage};
	}

	/**
	 * Invoke the specified listener method.
	 * @param methodName the name of the listener method
	 * @param arguments the message arguments to be passed in
	 * @param originalMessage the original message
	 * @return the result returned from the listener method
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #getListenerMethodName
	 * @see #buildListenerArguments
	 */
	protected Object invokeListenerMethod(String methodName, Object[] arguments, Message originalMessage)
			throws Exception {
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
				throw new ListenerExecutionFailedException("Listener method '" + methodName + "' threw exception",
						targetEx, originalMessage);
			}
		}
		catch (Exception ex) {
			ArrayList<String> arrayClass = new ArrayList<String>();
			if (arguments != null) {
				for (Object argument : arguments) {
					arrayClass.add(argument.getClass().toString());
				}
			}
			throw new ListenerExecutionFailedException("Failed to invoke target method '" + methodName
					+ "' with argument type = [" + StringUtils.collectionToCommaDelimitedString(arrayClass)
					+ "], value = [" + ObjectUtils.nullSafeToString(arguments) + "]", ex, originalMessage);
		}
	}

}
