/*
 * Copyright 2002-2016 the original author or authors.
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

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
			new String[] { "consumerCount", "messageCount" };

	private static final MethodFilter METHOD_FILTER = new MethodFilter() {

		@Override
		public boolean matches(java.lang.reflect.Method method) {
			return ObjectUtils.containsElement(METHODS_OF_INTEREST, method.getName());
		}

	};

	private static volatile java.lang.reflect.Method consumerCountMethod;

	private static volatile java.lang.reflect.Method messageCountMethod;

	private static volatile boolean conditionalMethodsChecked;

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Channel delegate;

	private final ConcurrentMap<String, Listener> listeners = new ConcurrentHashMap<String, Listener>();

	private final Map<Listener, SortedMap<Long, PendingConfirm>> pendingConfirms
		= new ConcurrentHashMap<PublisherCallbackChannel.Listener, SortedMap<Long,PendingConfirm>>();

	private final SortedMap<Long, Listener> listenerForSeq = new ConcurrentSkipListMap<Long, Listener>();

	public PublisherCallbackChannelImpl(Channel delegate) {
		delegate.addShutdownListener(this);
		this.delegate = delegate;

		if (!conditionalMethodsChecked) {
			// The following reflection is required to maintain compatibility with pre 3.6.x clients.
			ReflectionUtils.doWithMethods(delegate.getClass(), new MethodCallback(){

				@Override
				public void doWith(java.lang.reflect.Method method)
						throws IllegalArgumentException, IllegalAccessException {
					if ("consumerCount".equals(method.getName()) && method.getParameterTypes().length == 1
							&& String.class.equals(method.getParameterTypes()[0])
							&& long.class.equals(method.getReturnType())) {
						consumerCountMethod = method;
					}
					else if ("messageCount".equals(method.getName()) && method.getParameterTypes().length == 1
							&& String.class.equals(method.getParameterTypes()[0])
							&& long.class.equals(method.getReturnType())) {
						messageCountMethod = method;
					}
				}

			}, METHOD_FILTER);
			conditionalMethodsChecked = true;
		}
	}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BEGIN PURE DELEGATE METHODS
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void addShutdownListener(ShutdownListener listener) {
		this.delegate.addShutdownListener(listener);
	}

	@Override
	public void removeShutdownListener(ShutdownListener listener) {
		this.delegate.removeShutdownListener(listener);
	}

	@Override
	public ShutdownSignalException getCloseReason() {
		return this.delegate.getCloseReason();
	}

	@Override
	public void notifyListeners() {
		this.delegate.notifyListeners();
	}

	@Override
	public boolean isOpen() {
		return this.delegate.isOpen();
	}

	@Override
	public int getChannelNumber() {
		return this.delegate.getChannelNumber();
	}

	@Override
	public Connection getConnection() {
		return this.delegate.getConnection();
	}

	@Override
	public void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
		this.delegate.close(closeCode, closeMessage);
	}

	/**
	 * Added to the 3.3.x client
	 * @since 1.3.3
	 * @deprecated in the 3.5.3 client
	 */
	@Override
	@Deprecated
	public boolean flowBlocked() {
		return this.delegate.flowBlocked();
	}

	@Override
	public void abort() throws IOException {
		this.delegate.abort();
	}

	@Override
	public void abort(int closeCode, String closeMessage) throws IOException {
		this.delegate.abort(closeCode, closeMessage);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void addFlowListener(FlowListener listener) {
		this.delegate.addFlowListener(listener);
	}

	@Override
	@SuppressWarnings("deprecation")
	public boolean removeFlowListener(FlowListener listener) {
		return this.delegate.removeFlowListener(listener);
	}

	@Override
	@SuppressWarnings("deprecation")
	public void clearFlowListeners() {
		this.delegate.clearFlowListeners();
	}

	@Override
	public Consumer getDefaultConsumer() {
		return this.delegate.getDefaultConsumer();
	}

	@Override
	public void setDefaultConsumer(Consumer consumer) {
		this.delegate.setDefaultConsumer(consumer);
	}

	@Override
	public void basicQos(int prefetchSize, int prefetchCount, boolean global)
			throws IOException {
		this.delegate.basicQos(prefetchSize, prefetchCount, global);
	}

	/**
	 * Added to the 3.3.x client
	 * @since 1.3.3
	 */
	@Override
	public void basicQos(int prefetchCount, boolean global) throws IOException {
		this.delegate.basicQos(prefetchCount, global);
	}

	@Override
	public void basicQos(int prefetchCount) throws IOException {
		this.delegate.basicQos(prefetchCount);
	}

	@Override
	public void basicPublish(String exchange, String routingKey,
			BasicProperties props, byte[] body) throws IOException {
		this.delegate.basicPublish(exchange, routingKey, props, body);
	}

	@Override
	public void basicPublish(String exchange, String routingKey,
			boolean mandatory, boolean immediate, BasicProperties props,
			byte[] body) throws IOException {
		this.delegate.basicPublish(exchange, routingKey, mandatory, props, body);
	}

	@Override
	public void basicPublish(String exchange, String routingKey,
			boolean mandatory, BasicProperties props, byte[] body)
			throws IOException {
		this.delegate.basicPublish(exchange, routingKey, mandatory, props, body);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type)
			throws IOException {
		return this.delegate.exchangeDeclare(exchange, type);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable, boolean autoDelete, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable, autoDelete,
				arguments);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable, boolean autoDelete, boolean internal,
			Map<String, Object> arguments) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable, autoDelete,
				internal, arguments);
	}

	@Override
	public DeclareOk exchangeDeclarePassive(String name) throws IOException {
		return this.delegate.exchangeDeclarePassive(name);
	}

	@Override
	public DeleteOk exchangeDelete(String exchange, boolean ifUnused)
			throws IOException {
		return this.delegate.exchangeDelete(exchange, ifUnused);
	}

	@Override
	public DeleteOk exchangeDelete(String exchange) throws IOException {
		return this.delegate.exchangeDelete(exchange);
	}

	@Override
	public BindOk exchangeBind(String destination, String source,
			String routingKey) throws IOException {
		return this.delegate.exchangeBind(destination, source, routingKey);
	}

	@Override
	public BindOk exchangeBind(String destination, String source,
			String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate
				.exchangeBind(destination, source, routingKey, arguments);
	}

	@Override
	public UnbindOk exchangeUnbind(String destination, String source,
			String routingKey) throws IOException {
		return this.delegate.exchangeUnbind(destination, source, routingKey);
	}

	@Override
	public UnbindOk exchangeUnbind(String destination, String source,
			String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.exchangeUnbind(destination, source, routingKey,
				arguments);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare()
			throws IOException {
		return this.delegate.queueDeclare();
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare(String queue,
			boolean durable, boolean exclusive, boolean autoDelete,
			Map<String, Object> arguments) throws IOException {
		return this.delegate.queueDeclare(queue, durable, exclusive, autoDelete,
				arguments);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclarePassive(
			String queue) throws IOException {
		return this.delegate.queueDeclarePassive(queue);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue)
			throws IOException {
		return this.delegate.queueDelete(queue);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue,
			boolean ifUnused, boolean ifEmpty) throws IOException {
		return this.delegate.queueDelete(queue, ifUnused, ifEmpty);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue,
			String exchange, String routingKey) throws IOException {
		return this.delegate.queueBind(queue, exchange, routingKey);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue,
			String exchange, String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.queueBind(queue, exchange, routingKey, arguments);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue,
			String exchange, String routingKey) throws IOException {
		return this.delegate.queueUnbind(queue, exchange, routingKey);
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue,
			String exchange, String routingKey, Map<String, Object> arguments)
			throws IOException {
		return this.delegate.queueUnbind(queue, exchange, routingKey, arguments);
	}

	@Override
	public PurgeOk queuePurge(String queue) throws IOException {
		return this.delegate.queuePurge(queue);
	}

	@Override
	public GetResponse basicGet(String queue, boolean autoAck)
			throws IOException {
		return this.delegate.basicGet(queue, autoAck);
	}

	@Override
	public void basicAck(long deliveryTag, boolean multiple) throws IOException {
		this.delegate.basicAck(deliveryTag, multiple);
	}

	@Override
	public void basicNack(long deliveryTag, boolean multiple, boolean requeue)
			throws IOException {
		this.delegate.basicNack(deliveryTag, multiple, requeue);
	}

	@Override
	public void basicReject(long deliveryTag, boolean requeue)
			throws IOException {
		this.delegate.basicReject(deliveryTag, requeue);
	}

	@Override
	public String basicConsume(String queue, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, callback);
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, callback);
	}

	@Override
	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, Consumer callback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, callback);
	}

	/**
	 * Added to the 3.3.x client
	 * @since 1.3.3
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, arguments, callback);
	}

	@Override
	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, boolean noLocal, boolean exclusive,
			Map<String, Object> arguments, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, noLocal,
				exclusive, arguments, callback);
	}

	@Override
	public void basicCancel(String consumerTag) throws IOException {
		this.delegate.basicCancel(consumerTag);
	}

	@Override
	public RecoverOk basicRecover() throws IOException {
		return this.delegate.basicRecover();
	}

	@Override
	public RecoverOk basicRecover(boolean requeue) throws IOException {
		return this.delegate.basicRecover(requeue);
	}

	@Override
	public SelectOk txSelect() throws IOException {
		return this.delegate.txSelect();
	}

	@Override
	public CommitOk txCommit() throws IOException {
		return this.delegate.txCommit();
	}

	@Override
	public RollbackOk txRollback() throws IOException {
		return this.delegate.txRollback();
	}

	@Override
	public com.rabbitmq.client.AMQP.Confirm.SelectOk confirmSelect()
			throws IOException {
		return this.delegate.confirmSelect();
	}

	@Override
	public long getNextPublishSeqNo() {
		return this.delegate.getNextPublishSeqNo();
	}

	@Override
	public boolean waitForConfirms() throws InterruptedException {
		return this.delegate.waitForConfirms();
	}

	@Override
	public boolean waitForConfirms(long timeout) throws InterruptedException,
			TimeoutException {
		return this.delegate.waitForConfirms(timeout);
	}

	@Override
	public void waitForConfirmsOrDie() throws IOException, InterruptedException {
		this.delegate.waitForConfirmsOrDie();
	}

	@Override
	public void waitForConfirmsOrDie(long timeout) throws IOException,
			InterruptedException, TimeoutException {
		this.delegate.waitForConfirmsOrDie(timeout);
	}

	@Override
	public void asyncRpc(Method method) throws IOException {
		this.delegate.asyncRpc(method);
	}

	@Override
	public Command rpc(Method method) throws IOException {
		return this.delegate.rpc(method);
	}

	@Override
	public void addConfirmListener(ConfirmListener listener) {
		this.delegate.addConfirmListener(listener);
	}

	@Override
	public boolean removeConfirmListener(ConfirmListener listener) {
		return this.delegate.removeConfirmListener(listener);
	}

	@Override
	public void clearConfirmListeners() {
		this.delegate.clearConfirmListeners();
	}

	@Override
	public void addReturnListener(ReturnListener listener) {
		this.delegate.addReturnListener(listener);
	}

	@Override
	public boolean removeReturnListener(ReturnListener listener) {
		return this.delegate.removeReturnListener(listener);
	}

	@Override
	public synchronized void clearReturnListeners() {
		this.delegate.clearReturnListeners();
	}

	@Override
	public void exchangeBindNoWait(String destination, String source,
			String routingKey, Map<String, Object> arguments) throws IOException {
		this.delegate.exchangeBind(destination, source, routingKey, arguments);
	}

	@Override
	public void exchangeDeclareNoWait(String exchange, String type,
			boolean durable, boolean autoDelete, boolean internal,
			Map<String, Object> arguments) throws IOException {
		this.delegate.exchangeDeclareNoWait(exchange, type, durable, autoDelete, internal, arguments);
	}

	@Override
	public void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
		this.delegate.exchangeDeleteNoWait(exchange, ifUnused);
	}

	@Override
	public void exchangeUnbindNoWait(String destination, String source,
			String routingKey, Map<String, Object> arguments)
			throws IOException {
		this.delegate.exchangeUnbindNoWait(destination, source, routingKey, arguments);
	}

	@Override
	public void queueBindNoWait(String queue,
			String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
		this.delegate.queueBindNoWait(queue, exchange, routingKey, arguments);
	}

	@Override
	public void queueDeclareNoWait(String queue,
			boolean durable, boolean exclusive, boolean autoDelete,
			Map<String, Object> arguments)
			throws IOException {
		this.delegate.queueDeclareNoWait(queue, durable, exclusive, autoDelete, arguments);
	}

	@Override
	public void queueDeleteNoWait(String queue,
			boolean ifUnused, boolean ifEmpty) throws IOException {
		this.delegate.queueDeleteNoWait(queue, ifUnused, ifEmpty);
	}

	@Override
	public long consumerCount(String queue) throws IOException {
		if (consumerCountMethod != null) {
			return (Long) ReflectionUtils.invokeMethod(consumerCountMethod, this.delegate, new Object[] { queue });
		}
		throw new UnsupportedOperationException("'consumerCount()' requires a 3.6+ client library");
	}

	@Override
	public long messageCount(String queue) throws IOException {
		if (messageCountMethod != null) {
			return (Long) ReflectionUtils.invokeMethod(messageCountMethod, this.delegate, new Object[] { queue });
		}
		throw new UnsupportedOperationException("'messageCountMethod()' requires a 3.6+ client library");
	}

	@Override
	public Channel getDelegate() {
		return this.delegate;
	}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// END PURE DELEGATE METHODS
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void close() throws IOException, TimeoutException {
		try {
			this.delegate.close();
		}
		catch (AlreadyClosedException e) {
			if (this.logger.isTraceEnabled()) {
				this.logger.trace(this.delegate + " is already closed");
			}
		}
		generateNacksForPendingAcks("Channel closed by application");
	}

	private synchronized void generateNacksForPendingAcks(String cause) {
		for (Entry<Listener, SortedMap<Long, PendingConfirm>> entry : this.pendingConfirms.entrySet()) {
			Listener listener = entry.getKey();
			for (Entry<Long, PendingConfirm> confirmEntry : entry.getValue().entrySet()) {
				confirmEntry.getValue().setCause(cause);
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(this.toString() + " PC:Nack:(close):" + confirmEntry.getKey());
				}
				processAck(confirmEntry.getKey(), false, false, false);
			}
			listener.revoke(this);
		}
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("PendingConfirms cleared");
		}
		this.pendingConfirms.clear();
		this.listenerForSeq.clear();
		this.listeners.clear();
	}

	/**
	 * Add the listener and return the internal map of pending confirmations for that listener.
	 * @param listener the listener.
	 */
	@Override
	public void addListener(Listener listener) {
		Assert.notNull(listener, "Listener cannot be null");
		if (this.listeners.size() == 0) {
			this.delegate.addConfirmListener(this);
			this.delegate.addReturnListener(this);
		}
		if (this.listeners.putIfAbsent(listener.getUUID(), listener) == null) {
			this.pendingConfirms.put(listener, new ConcurrentSkipListMap<Long, PendingConfirm>());
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Added listener " + listener);
			}
		}
	}

	@Override
	public synchronized Collection<PendingConfirm> expire(Listener listener, long cutoffTime) {
		SortedMap<Long, PendingConfirm> pendingConfirmsForListener = this.pendingConfirms.get(listener);
		if (pendingConfirmsForListener == null) {
			return Collections.<PendingConfirm>emptyList();
		}
		else {
			List<PendingConfirm> expired = new ArrayList<PendingConfirm>();
			Iterator<Entry<Long, PendingConfirm>> iterator = pendingConfirmsForListener.entrySet().iterator();
			while (iterator.hasNext()) {
				PendingConfirm pendingConfirm = iterator.next().getValue();
				if (pendingConfirm.getTimestamp() < cutoffTime) {
					expired.add(pendingConfirm);
					iterator.remove();
				}
				else {
					break;
				}
			}
			return expired;
		}
	}

//	ConfirmListener

	@Override
	public void handleAck(long seq, boolean multiple)
			throws IOException {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(this.toString() + " PC:Ack:" + seq + ":" + multiple);
		}
		this.processAck(seq, true, multiple, true);
	}

	@Override
	public void handleNack(long seq, boolean multiple)
			throws IOException {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(this.toString() + " PC:Nack:" + seq + ":" + multiple);
		}
		this.processAck(seq, false, multiple, true);
	}

	private synchronized void processAck(long seq, boolean ack, boolean multiple, boolean remove) {
		if (multiple) {
			/*
			 * Piggy-backed ack - extract all Listeners for this and earlier
			 * sequences. Then, for each Listener, handle each of it's acks.
			 * Finally, remove the sequences from listenerForSeq.
			 */
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
						PendingConfirm value = entry.getValue();
						iterator.remove();
						doHandleConfirm(ack, involvedListener, value);
					}
				}
			}
			List<Long> seqs = new ArrayList<Long>(involvedListeners.keySet());
			for (Long key : seqs) {
				this.listenerForSeq.remove(key);
			}
		}
		else {
			Listener listener = this.listenerForSeq.remove(seq);
			if (listener != null) {
				SortedMap<Long, PendingConfirm> confirmsForListener = this.pendingConfirms.get(listener);
				PendingConfirm pendingConfirm;
				if (remove) {
					pendingConfirm = confirmsForListener.remove(seq);
				}
				else {
					pendingConfirm = confirmsForListener.get(seq);
				}
				if (pendingConfirm != null) {
					doHandleConfirm(ack, listener, pendingConfirm);
				}
			}
			else {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(this.delegate.toString() + " No listener for seq:" + seq);
				}
			}
		}
	}

	private void doHandleConfirm(boolean ack, Listener listener, PendingConfirm pendingConfirm) {
		try {
			if (listener.isConfirmListener()) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Sending confirm " + pendingConfirm);
				}
				listener.handleConfirm(pendingConfirm, ack);
			}
		}
		catch (Exception e) {
			this.logger.error("Exception delivering confirm", e);
		}
	}

	@Override
	public synchronized void addPendingConfirm(Listener listener, long seq, PendingConfirm pendingConfirm) {
		SortedMap<Long, PendingConfirm> pendingConfirmsForListener = this.pendingConfirms.get(listener);
		Assert.notNull(pendingConfirmsForListener,
				"Listener not registered: " + listener + " " + this.pendingConfirms.keySet());
		pendingConfirmsForListener.put(seq, pendingConfirm);
		this.listenerForSeq.put(seq, listener);
	}

//  ReturnListener

	@Override
	public void handleReturn(int replyCode,
			String replyText,
			String exchange,
			String routingKey,
			AMQP.BasicProperties properties,
			byte[] body) throws IOException
	{
		String uuidObject = properties.getHeaders().get(RETURN_CORRELATION_KEY).toString();
		Listener listener = this.listeners.get(uuidObject);
		if (listener == null || !listener.isReturnListener()) {
			if (this.logger.isWarnEnabled()) {
				this.logger.warn("No Listener for returned message");
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
