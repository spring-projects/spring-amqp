/*
 * Copyright 2002-2014 the original author or authors.
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
package org.springframework.amqp.rabbit.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.ReflectionUtils.MethodFilter;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Basic.RecoverOk;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Channel.FlowOk;
import com.rabbitmq.client.AMQP.Exchange.BindOk;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.AMQP.Exchange.UnbindOk;
import com.rabbitmq.client.AMQP.Queue.PurgeOk;
import com.rabbitmq.client.AMQP.Tx.CommitOk;
import com.rabbitmq.client.AMQP.Tx.RollbackOk;
import com.rabbitmq.client.AMQP.Tx.SelectOk;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.FlowListener;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Channel wrapper to allow a single listener able to handle
 * confirms from multiple channels.
 *
 * @author Gary Russell
 * @since 1.0.1
 *
 */
public class PublisherCallbackChannelImpl
		implements PublisherCallbackChannel, ConfirmListener, ReturnListener, ShutdownListener {

	private static final String[] METHODS_OF_INTEREST =
			new String[] {"getFlow", "flow", "flowBlocked", "basicConsume", "basicQos"};

	private static final MethodFilter METHOD_FILTER = new MethodFilter() {

		@Override
		public boolean matches(java.lang.reflect.Method method) {
			return ObjectUtils.containsElement(METHODS_OF_INTEREST, method.getName());
		}

	};

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Channel delegate;

	private final Map<String, Listener> listeners = new ConcurrentHashMap<String, Listener>();

	private final Map<Listener, SortedMap<Long, PendingConfirm>> pendingConfirms
		= new ConcurrentHashMap<PublisherCallbackChannel.Listener, SortedMap<Long,PendingConfirm>>();

	private final SortedMap<Long, Listener> listenerForSeq = new ConcurrentSkipListMap<Long, Listener>();

	private final java.lang.reflect.Method getFlowMethod;

	private final java.lang.reflect.Method flowMethod;

	private final java.lang.reflect.Method flowBlockedMethod;

	private final java.lang.reflect.Method basicConsumeFourArgsMethod;

	private final java.lang.reflect.Method basicQosTwoArgsMethod;

	public PublisherCallbackChannelImpl(Channel delegate) {
		delegate.addShutdownListener(this);
		this.delegate = delegate;

		// The following reflection is required to maintain compatibility with pre 3.3.x clients.
		final AtomicReference<java.lang.reflect.Method> getFlowMethod = new AtomicReference<java.lang.reflect.Method>();
		final AtomicReference<java.lang.reflect.Method> flowMethod = new AtomicReference<java.lang.reflect.Method>();
		final AtomicReference<java.lang.reflect.Method> flowBlockedMethod = new AtomicReference<java.lang.reflect.Method>();
		final AtomicReference<java.lang.reflect.Method> basicConsumeFourArgsMethod = new AtomicReference<java.lang.reflect.Method>();
		final AtomicReference<java.lang.reflect.Method> basicQosTwoArgsMethod = new AtomicReference<java.lang.reflect.Method>();
		ReflectionUtils.doWithMethods(delegate.getClass(), new MethodCallback(){

			@Override
			public void doWith(java.lang.reflect.Method method) throws IllegalArgumentException, IllegalAccessException {
				if ("getFlow".equals(method.getName()) && method.getParameterTypes().length == 0
						&& FlowOk.class.equals(method.getReturnType())) {
					getFlowMethod.set(method);
				}
				else if ("flow".equals(method.getName()) && method.getParameterTypes().length == 1
						&& boolean.class.equals(method.getParameterTypes()[0])
						&& FlowOk.class.equals(method.getReturnType())) {
					flowMethod.set(method);
				}
				else if ("flowBlocked".equals(method.getName()) && method.getParameterTypes().length == 0
						&& boolean.class.equals(method.getReturnType())) {
					flowBlockedMethod.set(method);
				}
				else if ("basicConsume".equals(method.getName()) && method.getParameterTypes().length == 4
						&& String.class.equals(method.getParameterTypes()[0])
						&& boolean.class.equals(method.getParameterTypes()[1])
						&& Map.class.equals(method.getParameterTypes()[2])
						&& Consumer.class.equals(method.getParameterTypes()[3])
						&& String.class.equals(method.getReturnType())) {
					basicConsumeFourArgsMethod.set(method);
				}
				else if ("basicQos".equals(method.getName()) && method.getParameterTypes().length == 2
						&& int.class.equals(method.getParameterTypes()[0])
						&& boolean.class.equals(method.getParameterTypes()[1])
						&& void.class.equals(method.getReturnType())) {
					basicQosTwoArgsMethod.set(method);
				}
			}

		}, METHOD_FILTER);
		this.getFlowMethod = getFlowMethod.get();
		this.flowMethod = flowMethod.get();
		this.flowBlockedMethod = flowBlockedMethod.get();
		this.basicConsumeFourArgsMethod = basicConsumeFourArgsMethod.get();
		this.basicQosTwoArgsMethod = basicQosTwoArgsMethod.get();
	}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BEGIN PURE DELEGATE METHODS
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public void addShutdownListener(ShutdownListener listener) {
		this.delegate.addShutdownListener(listener);
	}

	public void removeShutdownListener(ShutdownListener listener) {
		this.delegate.removeShutdownListener(listener);
	}

	public ShutdownSignalException getCloseReason() {
		return this.delegate.getCloseReason();
	}

	public void notifyListeners() {
		this.delegate.notifyListeners();
	}

	public boolean isOpen() {
		return this.delegate.isOpen();
	}

	public int getChannelNumber() {
		return this.delegate.getChannelNumber();
	}

	public Connection getConnection() {
		return this.delegate.getConnection();
	}

	public void close(int closeCode, String closeMessage) throws IOException {
		this.delegate.close(closeCode, closeMessage);
	}

	/**
	 * @deprecated - removed in the 3.3.x client
	 * @param active active.
	 * @return FlowOk.
	 * @throws IOException IOException.
	 */
	@Deprecated
	public FlowOk flow(boolean active) throws IOException {
		if (this.flowMethod != null) {
			return (FlowOk) ReflectionUtils.invokeMethod(this.flowMethod, this.delegate, active);
		}
		throw new UnsupportedOperationException("'flow(boolean)' is not supported by the client library");
	}

	/**
	 * @deprecated - removed in the 3.3.x client
	 * @return FlowOk.
	 */
	@Deprecated
	public FlowOk getFlow() {
		if (this.getFlowMethod != null) {
			return (FlowOk) ReflectionUtils.invokeMethod(this.getFlowMethod, this.delegate);
		}
		throw new UnsupportedOperationException("'getFlow()' is not supported by the client library");
	}

	/**
	 * Added to the 3.3.x client
	 * @since 1.3.3
	 */
	public boolean flowBlocked() {
		if (this.flowBlockedMethod != null) {
			return (Boolean) ReflectionUtils.invokeMethod(this.flowBlockedMethod, this.delegate);
		}
		throw new UnsupportedOperationException("'flowBlocked()' is not supported by the client library");
	}

	public void abort() throws IOException {
		this.delegate.abort();
	}

	public void abort(int closeCode, String closeMessage) throws IOException {
		this.delegate.abort(closeCode, closeMessage);
	}

	public void addFlowListener(FlowListener listener) {
		this.delegate.addFlowListener(listener);
	}

	public boolean removeFlowListener(FlowListener listener) {
		return this.delegate.removeFlowListener(listener);
	}

	public void clearFlowListeners() {
		this.delegate.clearFlowListeners();
	}

	public Consumer getDefaultConsumer() {
		return this.delegate.getDefaultConsumer();
	}

	public void setDefaultConsumer(Consumer consumer) {
		this.delegate.setDefaultConsumer(consumer);
	}

	public void basicQos(int prefetchSize, int prefetchCount, boolean global)
			throws IOException {
		this.delegate.basicQos(prefetchSize, prefetchCount, global);
	}

	/**
	 * Added to the 3.3.x client
	 * @since 1.3.3
	 */
	public void basicQos(int prefetchCount, boolean global) throws IOException {
		if (this.basicQosTwoArgsMethod != null) {
			ReflectionUtils.invokeMethod(this.basicQosTwoArgsMethod, this.delegate, prefetchCount,
					global);
			return;
		}
		throw new UnsupportedOperationException("'basicQos(int, boolean)' is not supported by the client library");
	}

	public void basicQos(int prefetchCount) throws IOException {
		this.delegate.basicQos(prefetchCount);
	}

	public void basicPublish(String exchange, String routingKey,
			BasicProperties props, byte[] body) throws IOException {
		this.delegate.basicPublish(exchange, routingKey, props, body);
	}

	public void basicPublish(String exchange, String routingKey,
			boolean mandatory, boolean immediate, BasicProperties props,
			byte[] body) throws IOException {
		this.delegate.basicPublish(exchange, routingKey, mandatory, props, body);
	}

	public void basicPublish(String exchange, String routingKey,
			boolean mandatory, BasicProperties props, byte[] body)
			throws IOException {
		this.delegate.basicPublish(exchange, routingKey, mandatory, props, body);
	}

	public DeclareOk exchangeDeclare(String exchange, String type)
			throws IOException {
		return this.delegate.exchangeDeclare(exchange, type);
	}

	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable);
	}

	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable, boolean autoDelete, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable, autoDelete,
				arguments);
	}

	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable, boolean autoDelete, boolean internal,
			Map<String, Object> arguments) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable, autoDelete,
				internal, arguments);
	}

	public DeclareOk exchangeDeclarePassive(String name) throws IOException {
		return this.delegate.exchangeDeclarePassive(name);
	}

	public DeleteOk exchangeDelete(String exchange, boolean ifUnused)
			throws IOException {
		return this.delegate.exchangeDelete(exchange, ifUnused);
	}

	public DeleteOk exchangeDelete(String exchange) throws IOException {
		return this.delegate.exchangeDelete(exchange);
	}

	public BindOk exchangeBind(String destination, String source,
			String routingKey) throws IOException {
		return this.delegate.exchangeBind(destination, source, routingKey);
	}

	public BindOk exchangeBind(String destination, String source,
			String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate
				.exchangeBind(destination, source, routingKey, arguments);
	}

	public UnbindOk exchangeUnbind(String destination, String source,
			String routingKey) throws IOException {
		return this.delegate.exchangeUnbind(destination, source, routingKey);
	}

	public UnbindOk exchangeUnbind(String destination, String source,
			String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.exchangeUnbind(destination, source, routingKey,
				arguments);
	}

	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare()
			throws IOException {
		return this.delegate.queueDeclare();
	}

	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare(String queue,
			boolean durable, boolean exclusive, boolean autoDelete,
			Map<String, Object> arguments) throws IOException {
		return this.delegate.queueDeclare(queue, durable, exclusive, autoDelete,
				arguments);
	}

	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclarePassive(
			String queue) throws IOException {
		return this.delegate.queueDeclarePassive(queue);
	}

	public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue)
			throws IOException {
		return this.delegate.queueDelete(queue);
	}

	public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue,
			boolean ifUnused, boolean ifEmpty) throws IOException {
		return this.delegate.queueDelete(queue, ifUnused, ifEmpty);
	}

	public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue,
			String exchange, String routingKey) throws IOException {
		return this.delegate.queueBind(queue, exchange, routingKey);
	}

	public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue,
			String exchange, String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.queueBind(queue, exchange, routingKey, arguments);
	}

	public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue,
			String exchange, String routingKey) throws IOException {
		return this.delegate.queueUnbind(queue, exchange, routingKey);
	}

	public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue,
			String exchange, String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.queueUnbind(queue, exchange, routingKey, arguments);
	}

	public PurgeOk queuePurge(String queue) throws IOException {
		return this.delegate.queuePurge(queue);
	}

	public GetResponse basicGet(String queue, boolean autoAck)
			throws IOException {
		return this.delegate.basicGet(queue, autoAck);
	}

	public void basicAck(long deliveryTag, boolean multiple) throws IOException {
		this.delegate.basicAck(deliveryTag, multiple);
	}

	public void basicNack(long deliveryTag, boolean multiple, boolean requeue)
			throws IOException {
		this.delegate.basicNack(deliveryTag, multiple, requeue);
	}

	public void basicReject(long deliveryTag, boolean requeue)
			throws IOException {
		this.delegate.basicReject(deliveryTag, requeue);
	}

	public String basicConsume(String queue, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, callback);
	}

	public String basicConsume(String queue, boolean autoAck, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, callback);
	}

	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, Consumer callback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, callback);
	}

	/**
	 * Added to the 3.3.x client
	 * @since 1.3.3
	 */
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback)
			throws IOException {
		if (this.basicConsumeFourArgsMethod != null) {
			return (String) ReflectionUtils.invokeMethod(this.basicConsumeFourArgsMethod, this.delegate, queue,
					autoAck, arguments, callback);
		}
		throw new UnsupportedOperationException("'basicConsume(String, boolean, Map, Consumer)' " +
				"is not supported by the client library");
	}

	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, boolean noLocal, boolean exclusive,
			Map<String, Object> arguments, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, noLocal,
				exclusive, arguments, callback);
	}

	public void basicCancel(String consumerTag) throws IOException {
		this.delegate.basicCancel(consumerTag);
	}

	public RecoverOk basicRecover() throws IOException {
		return this.delegate.basicRecover();
	}

	public RecoverOk basicRecover(boolean requeue) throws IOException {
		return this.delegate.basicRecover(requeue);
	}

	@Deprecated
	public void basicRecoverAsync(boolean requeue) throws IOException {
		this.delegate.basicRecover(requeue);
	}

	public SelectOk txSelect() throws IOException {
		return this.delegate.txSelect();
	}

	public CommitOk txCommit() throws IOException {
		return this.delegate.txCommit();
	}

	public RollbackOk txRollback() throws IOException {
		return this.delegate.txRollback();
	}

	public com.rabbitmq.client.AMQP.Confirm.SelectOk confirmSelect()
			throws IOException {
		return this.delegate.confirmSelect();
	}

	public long getNextPublishSeqNo() {
		return this.delegate.getNextPublishSeqNo();
	}

	public boolean waitForConfirms() throws InterruptedException {
		return this.delegate.waitForConfirms();
	}

	public boolean waitForConfirms(long timeout) throws InterruptedException,
			TimeoutException {
		return this.delegate.waitForConfirms(timeout);
	}

	public void waitForConfirmsOrDie() throws IOException, InterruptedException {
		this.delegate.waitForConfirmsOrDie();
	}

	public void waitForConfirmsOrDie(long timeout) throws IOException,
			InterruptedException, TimeoutException {
		this.delegate.waitForConfirmsOrDie(timeout);
	}

	public void asyncRpc(Method method) throws IOException {
		this.delegate.asyncRpc(method);
	}

	public Command rpc(Method method) throws IOException {
		return this.delegate.rpc(method);
	}

	public void addConfirmListener(ConfirmListener listener) {
		this.delegate.addConfirmListener(listener);
	}

	public boolean removeConfirmListener(ConfirmListener listener) {
		return this.delegate.removeConfirmListener(listener);
	}

	public void clearConfirmListeners() {
		this.delegate.clearConfirmListeners();
	}

	public void addReturnListener(ReturnListener listener) {
		this.delegate.addReturnListener(listener);
	}

	public boolean removeReturnListener(ReturnListener listener) {
		return this.delegate.removeReturnListener(listener);
	}

	public synchronized void clearReturnListeners() {
		this.delegate.clearReturnListeners();
	}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// END PURE DELEGATE METHODS
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public void close() throws IOException {
		try {
			this.delegate.close();
		}
		catch (AlreadyClosedException e) {
			if (logger.isTraceEnabled()) {
				logger.trace(this.delegate + " is already closed");
			}
		}
		generateNacksForPendingAcks("Channel closed by application");
	}

	private void generateNacksForPendingAcks(String cause) {
		synchronized (this.pendingConfirms) {
			for (Entry<Listener, SortedMap<Long, PendingConfirm>> entry : this.pendingConfirms.entrySet()) {
				Listener listener = entry.getKey();
				for (Entry<Long, PendingConfirm> confirmEntry : entry.getValue().entrySet()) {
					try {
						confirmEntry.getValue().setCause(cause);
						handleNack(confirmEntry.getKey(), false);
					}
					catch (IOException e) {
						logger.error("Error delivering Nack afterShutdown", e);
					}
				}
				listener.removePendingConfirmsReference(this, entry.getValue());
			}
			this.pendingConfirms.clear();
			this.listenerForSeq.clear();
			this.listeners.clear();
		}
	}

	public synchronized SortedMap<Long, PendingConfirm> addListener(Listener listener) {
		Assert.notNull(listener, "Listener cannot be null");
		if (this.listeners.size() == 0) {
			this.delegate.addConfirmListener(this);
			this.delegate.addReturnListener(this);
		}
		if (!this.listeners.values().contains(listener)){
			this.listeners.put(listener.getUUID(), listener);
			this.pendingConfirms.put(listener, Collections.synchronizedSortedMap(new TreeMap<Long, PendingConfirm>()));
			if (logger.isDebugEnabled()) {
				logger.debug("Added listener " + listener);
			}
		}
		return this.pendingConfirms.get(listener);
	}

	public synchronized boolean removeListener(Listener listener) {
		Listener mappedListener = this.listeners.remove(listener.getUUID());
		boolean result = mappedListener != null;
		if (result && this.listeners.size() == 0) {
			this.delegate.removeConfirmListener(this);
			this.delegate.removeReturnListener(this);
		}
		Iterator<Entry<Long, Listener>> iterator = this.listenerForSeq.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<Long, Listener> entry = iterator.next();
			if (entry.getValue() == listener) {
				iterator.remove();
			}
		}
		this.pendingConfirms.remove(listener);
		return result;
	}


//	ConfirmListener

	public void handleAck(long seq, boolean multiple)
			throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug(this.toString() + " PC:Ack:" + seq + ":" + multiple);
		}
		this.processAck(seq, true, multiple);
	}

	public void handleNack(long seq, boolean multiple)
			throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug(this.toString() + " PC:Nack:" + seq + ":" + multiple);
		}
		this.processAck(seq, false, multiple);
	}

	private void processAck(long seq, boolean ack, boolean multiple) {
		if (multiple) {
			/*
			 * Piggy-backed ack - extract all Listeners for this and earlier
			 * sequences. Then, for each Listener, handle each of it's acks.
			 * Finally, remove the sequences from listenerForSeq.
			 */
			synchronized(this.pendingConfirms) {
				Map<Long, Listener> involvedListeners = this.listenerForSeq.headMap(seq + 1);
				// eliminate duplicates
				Set<Listener> listeners = new HashSet<Listener>(involvedListeners.values());
				for (Listener involvedListener : listeners) {
					// find all unack'd confirms for this listener and handle them
					SortedMap<Long, PendingConfirm> confirmsMap = this.pendingConfirms.get(involvedListener);
					if (confirmsMap != null) {
						Map<Long, PendingConfirm> confirms = confirmsMap.headMap(seq + 1);
						Iterator<Entry<Long, PendingConfirm>> iterator = confirms.entrySet().iterator();
						while (iterator.hasNext()) {
							Entry<Long, PendingConfirm> entry = iterator.next();
							iterator.remove();
							doHandleConfirm(ack, involvedListener, entry.getValue());
						}
					}
				}
				List<Long> seqs = new ArrayList<Long>(involvedListeners.keySet());
				for (Long key : seqs) {
					this.listenerForSeq.remove(key);
				}
			}
		}
		else {
			Listener listener = this.listenerForSeq.remove(seq);
			if (listener != null) {
				PendingConfirm pendingConfirm = this.pendingConfirms.get(listener).remove(seq);
				if (pendingConfirm != null) {
					doHandleConfirm(ack, listener, pendingConfirm);
				}
			}
			else {
				logger.error("No listener for seq:" + seq);
			}
		}
	}

	private void doHandleConfirm(boolean ack, Listener listener, PendingConfirm pendingConfirm) {
		try {
			if (listener.isConfirmListener()) {
				if (logger.isDebugEnabled()) {
					logger.debug("Sending confirm " + pendingConfirm);
				}
				listener.handleConfirm(pendingConfirm, ack);
			}
		}
		catch (Exception e) {
			logger.error("Exception delivering confirm", e);
		}
	}

	public void addPendingConfirm(Listener listener, long seq, PendingConfirm pendingConfirm) {
		SortedMap<Long, PendingConfirm> pendingConfirmsForListener = this.pendingConfirms.get(listener);
		Assert.notNull(pendingConfirmsForListener, "Listener not registered");
		synchronized (this.pendingConfirms) {
			pendingConfirmsForListener.put(seq, pendingConfirm);
		}
		this.listenerForSeq.put(seq, listener);
	}

//  ReturnListener

	public void handleReturn(int replyCode,
			String replyText,
			String exchange,
			String routingKey,
			AMQP.BasicProperties properties,
			byte[] body) throws IOException
	{
		String uuidObject = properties.getHeaders().get(RETURN_CORRELATION).toString();
		Listener listener = this.listeners.get(uuidObject);
		if (listener == null || !listener.isReturnListener()) {
			if (logger.isWarnEnabled()) {
				logger.warn("No Listener for returned message");
			}
		}
		else {
			listener.handleReturn(replyCode, replyText, exchange, routingKey, properties, body);
		}
	}

// ShutdownListener

	@Override
	public void shutdownCompleted(ShutdownSignalException cause) {
		generateNacksForPendingAcks(cause.getMessage());
	}

// Object

	@Override
	public int hashCode() {
		return this.delegate.hashCode();
	}


	@Override
	public boolean equals(Object obj) {
		return obj == this || this.delegate.equals(obj);
	}

	@Override
	public String toString() {
		return "PublisherCallbackChannelImpl: " + this.delegate.toString();
	}

}
