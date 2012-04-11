/*
 * Copyright 2002-2012 the original author or authors.
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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

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
public class PublisherCallbackChannelImpl implements PublisherCallbackChannel, ConfirmListener, ReturnListener {

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Channel delegate;

	private final Map<String, Listener> listeners = new ConcurrentHashMap<String, Listener>();

	private final Map<Listener, SortedMap<Long, PendingConfirm>> pendingConfirms
		= new ConcurrentHashMap<PublisherCallbackChannel.Listener, SortedMap<Long,PendingConfirm>>();

	private final Map<Long, Listener> listenerForSeq = new ConcurrentHashMap<Long, Listener>();

	public PublisherCallbackChannelImpl(Channel delegate) {
		this.delegate = delegate;
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

	public FlowOk flow(boolean active) throws IOException {
		return this.delegate.flow(active);
	}

	public FlowOk getFlow() {
		return this.delegate.getFlow();
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
		this.delegate.basicPublish(exchange, routingKey, mandatory, immediate,
				props, body);
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

	@SuppressWarnings("deprecation")
	public void basicRecoverAsync(boolean requeue) throws IOException {
		this.delegate.basicRecoverAsync(requeue);
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
		this.delegate.close();
		for (Entry<Listener, SortedMap<Long, PendingConfirm>> entry : this.pendingConfirms.entrySet()) {
			Listener listener = entry.getKey();
			listener.removePendingConfirmsReference(this, entry.getValue());
		}
		this.pendingConfirms.clear();
		this.listenerForSeq.clear();
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
		Listener listener = this.listenerForSeq.get(seq);
		if (listener != null && listener.isConfirmListener()) {
			if (multiple) {
				Map<Long, PendingConfirm> headMap = this.pendingConfirms.get(listener).headMap(seq + 1);
				synchronized(this.pendingConfirms) {
					Iterator<Entry<Long, PendingConfirm>> iterator = headMap.entrySet().iterator();
					while (iterator.hasNext()) {
						Entry<Long, PendingConfirm> entry = iterator.next();
						iterator.remove();
						listener.handleConfirm(entry.getValue(), ack);
					}
				}
			}
			else {
				PendingConfirm pendingConfirm = this.pendingConfirms.get(listener).remove(seq);
				if (pendingConfirm != null) {
					listener.handleConfirm(pendingConfirm, ack);
				}
			}
		} else {
			logger.error("No listener for seq:" + seq);
		}
	}

	public void addPendingConfirm(Listener listener, long seq, PendingConfirm pendingConfirm) {
		SortedMap<Long, PendingConfirm> pendingConfirmsForListener = this.pendingConfirms.get(listener);
		Assert.notNull(pendingConfirmsForListener, "Listener not registered");
		pendingConfirmsForListener.put(seq, pendingConfirm);
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
		Object uuidObject = properties.getHeaders().get(RETURN_CORRELATION).toString();
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

// Object

	@Override
	public int hashCode() {
		return this.delegate.hashCode();
	}


	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		return this.delegate.equals(obj);
	}

	@Override
	public String toString() {
		return "PublisherCallbackChannelImpl: " + this.delegate.toString();
	}

}
