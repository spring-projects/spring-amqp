/*
 * Copyright 2002-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData.Confirm;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

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
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;

/**
 * Channel wrapper to allow a single listener able to handle
 * confirms from multiple channels.
 *
 * @author Gary Russell
 * @author Arnaud Cogoluègnes
 * @author Artem Bilan
 *
 * @since 1.0.1
 *
 */
public class PublisherCallbackChannelImpl
		implements PublisherCallbackChannel, ConfirmListener, ReturnCallback, ShutdownListener {

	private static final MessagePropertiesConverter CONVERTER  = new DefaultMessagePropertiesConverter();

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Channel delegate;

	private final ConcurrentMap<String, Listener> listeners = new ConcurrentHashMap<>();

	private final Map<Listener, SortedMap<Long, PendingConfirm>> pendingConfirms = new ConcurrentHashMap<>();

	private final Map<String, PendingConfirm> pendingReturns = new ConcurrentHashMap<>();

	private final SortedMap<Long, Listener> listenerForSeq = new ConcurrentSkipListMap<>();

	private final ExecutorService executor;

	private volatile java.util.function.Consumer<Channel> afterAckCallback;

	/**
	 * Create a {@link PublisherCallbackChannelImpl} instance based on the provided
	 * delegate and executor.
	 * @param delegate the delegate channel.
	 * @param executor the executor.
	 */
	public PublisherCallbackChannelImpl(Channel delegate, ExecutorService executor) {
		Assert.notNull(executor, "'executor' must not be null");
		this.delegate = delegate;
		this.executor = executor;
		delegate.addShutdownListener(this);
	}

	@Override
	public synchronized void setAfterAckCallback(java.util.function.Consumer<Channel> callback) {
		if (getPendingConfirmsCount() == 0 && callback != null) {
			callback.accept(this);
		}
		else {
			this.afterAckCallback = callback;
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
		if (this.delegate instanceof AutorecoveringChannel) {
			ClosingRecoveryListener.removeChannel((AutorecoveringChannel) this.delegate);
		}
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
	 * Added to the 3.3.x client.
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
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) throws IOException {
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
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
			Map<String, Object> arguments) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable, autoDelete, arguments);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type,
			boolean durable, boolean autoDelete, boolean internal,
			Map<String, Object> arguments) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable, autoDelete,
				internal, arguments);
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
			boolean internal, Map<String, Object> arguments) throws IOException {
		return this.delegate.exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
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

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
		return this.delegate.basicConsume(queue, deliverCallback, cancelCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, deliverCallback, shutdownSignalCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, deliverCallback, cancelCallback, shutdownSignalCallback);
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, callback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, deliverCallback, cancelCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, deliverCallback, shutdownSignalCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, deliverCallback, cancelCallback, shutdownSignalCallback);
	}

	@Override
	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, Consumer callback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, callback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, deliverCallback, cancelCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, deliverCallback, shutdownSignalCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, deliverCallback, cancelCallback, shutdownSignalCallback);
	}

	/**
	 * Added to the 3.3.x client.
	 * @since 1.3.3
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, arguments, callback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, arguments, deliverCallback, cancelCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, arguments, deliverCallback, shutdownSignalCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, arguments, deliverCallback, cancelCallback, shutdownSignalCallback);
	}

	@Override
	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, boolean noLocal, boolean exclusive,
			Map<String, Object> arguments, Consumer callback)
			throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, noLocal,
				exclusive, arguments, callback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments,
			DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, cancelCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments,
			DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, shutdownSignalCallback);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments,
			DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		return this.delegate.basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, cancelCallback, shutdownSignalCallback);
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
	public ConfirmListener addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
		return this.delegate.addConfirmListener(ackCallback, nackCallback);
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

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public ReturnListener addReturnListener(ReturnCallback returnCallback) {
		return this.delegate.addReturnListener(returnCallback);
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
	public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete,
			boolean internal, Map<String, Object> arguments) throws IOException {
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
		return this.delegate.consumerCount(queue);
	}

	/**
	 * Added to the 5.0.x client.
	 * @since 2.0
	 */
	@Override
	public CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
		return this.delegate.asyncCompletableRpc(method);
	}

	@Override
	public long messageCount(String queue) throws IOException {
		return this.delegate.messageCount(queue);
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
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Closing " + this.delegate);
		}
		try {
			this.delegate.close();
		}
		catch (AlreadyClosedException e) {
			if (this.logger.isTraceEnabled()) {
				this.logger.trace(this.delegate + " is already closed");
			}
		}
		shutdownCompleted("Channel closed by application");
	}

	private void shutdownCompleted(String cause) {
		this.executor.execute(() -> generateNacksForPendingAcks(cause));
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

	@Override
	public synchronized int getPendingConfirmsCount(Listener listener) {
		SortedMap<Long, PendingConfirm> pendingConfirmsForListener = this.pendingConfirms.get(listener);
		if (pendingConfirmsForListener == null) {
			return 0;
		}
		else {
			return pendingConfirmsForListener.entrySet().size();
		}
	}

	@Override
	public synchronized int getPendingConfirmsCount() {
		return this.pendingConfirms.values().stream()
				.mapToInt(Map::size)
				.sum();
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
					CorrelationData correlationData = pendingConfirm.getCorrelationData();
					if (correlationData != null && StringUtils.hasText(correlationData.getId())) {
						this.pendingReturns.remove(correlationData.getId()); // NOSONAR never null
					}
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
	public void handleAck(long seq, boolean multiple) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(this.toString() + " PC:Ack:" + seq + ":" + multiple);
		}
		processAck(seq, true, multiple, true);
	}

	@Override
	public void handleNack(long seq, boolean multiple) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(this.toString() + " PC:Nack:" + seq + ":" + multiple);
		}
		processAck(seq, false, multiple, true);
	}

	private synchronized void processAck(long seq, boolean ack, boolean multiple, boolean remove) {
		try {
			doProcessAck(seq, ack, multiple, remove);
		}
		catch (Exception e) {
			this.logger.error("Failed to process publisher confirm", e);
		}
	}

	private void doProcessAck(long seq, boolean ack, boolean multiple, boolean remove) {
		if (multiple) {
			processMultipleAck(seq, ack);
		}
		else {
			Listener listener = this.listenerForSeq.remove(seq);
			if (listener != null) {
				SortedMap<Long, PendingConfirm> confirmsForListener = this.pendingConfirms.get(listener);
				PendingConfirm pendingConfirm = null;
				if (confirmsForListener != null) { // should never happen; defensive
					if (remove) {
						pendingConfirm = confirmsForListener.remove(seq);
					}
					else {
						pendingConfirm = confirmsForListener.get(seq);
					}
				}
				if (pendingConfirm != null) {
					CorrelationData correlationData = pendingConfirm.getCorrelationData();
					if (correlationData != null) {
						correlationData.getFuture().set(new Confirm(ack, pendingConfirm.getCause()));
						if (StringUtils.hasText(correlationData.getId())) {
							this.pendingReturns.remove(correlationData.getId()); // NOSONAR never null
						}
					}
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

	private void processMultipleAck(long seq, boolean ack) {
		/*
		 * Piggy-backed ack - extract all Listeners for this and earlier
		 * sequences. Then, for each Listener, handle each of it's acks.
		 * Finally, remove the sequences from listenerForSeq.
		 */
		Map<Long, Listener> involvedListeners = this.listenerForSeq.headMap(seq + 1);
		// eliminate duplicates
		Set<Listener> listenersForAcks = new HashSet<Listener>(involvedListeners.values());
		for (Listener involvedListener : listenersForAcks) {
			// find all unack'd confirms for this listener and handle them
			SortedMap<Long, PendingConfirm> confirmsMap = this.pendingConfirms.get(involvedListener);
			if (confirmsMap != null) {
				Map<Long, PendingConfirm> confirms = confirmsMap.headMap(seq + 1);
				Iterator<Entry<Long, PendingConfirm>> iterator = confirms.entrySet().iterator();
				while (iterator.hasNext()) {
					Entry<Long, PendingConfirm> entry = iterator.next();
					PendingConfirm value = entry.getValue();
					CorrelationData correlationData = value.getCorrelationData();
					if (correlationData != null) {
						correlationData.getFuture().set(new Confirm(ack, value.getCause()));
						if (StringUtils.hasText(correlationData.getId())) {
							this.pendingReturns.remove(correlationData.getId()); // NOSONAR never null
						}
					}
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

	private void doHandleConfirm(boolean ack, Listener listener, PendingConfirm pendingConfirm) {
		this.executor.execute(() -> {
			try {
				if (listener.isConfirmListener()) {
					if (pendingConfirm.isReturned() && !pendingConfirm.waitForReturnIfNeeded()) {
						this.logger.error("Return callback failed to execute in "
								+ PendingConfirm.RETURN_CALLBACK_TIMEOUT + " seconds");
					}
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Sending confirm " + pendingConfirm);
					}
					listener.handleConfirm(pendingConfirm, ack);
				}
			}
			catch (Exception e) {
				this.logger.error("Exception delivering confirm", e);
			}
			finally {
				try {
					if (this.afterAckCallback != null) {
						java.util.function.Consumer<Channel> callback = null;
						synchronized (this) {
							if (getPendingConfirmsCount() == 0) {
								callback = this.afterAckCallback;
								this.afterAckCallback = null;
							}
						}
						if (callback != null) {
							callback.accept(this);
						}

					}
				}
				catch (Exception e) {
					this.logger.error("Failed to invoke afterAckCallback", e);
				}
			}
		});
	}

	@Override
	public synchronized void addPendingConfirm(Listener listener, long seq, PendingConfirm pendingConfirm) {
		SortedMap<Long, PendingConfirm> pendingConfirmsForListener = this.pendingConfirms.get(listener);
		Assert.notNull(pendingConfirmsForListener,
				"Listener not registered: " + listener + " " + this.pendingConfirms.keySet());
		pendingConfirmsForListener.put(seq, pendingConfirm);
		this.listenerForSeq.put(seq, listener);
		if (pendingConfirm.getCorrelationData() != null) {
			String returnCorrelation = pendingConfirm.getCorrelationData().getId(); // NOSONAR never null
			if (StringUtils.hasText(returnCorrelation)) {
				this.pendingReturns.put(returnCorrelation, pendingConfirm);
			}
		}
	}

//  ReturnListener

	@Override
	public void handle(Return returned) {

		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Return " + this.toString());
		}
		PendingConfirm confirm = findConfirm(returned);
		Listener listener = findListener(returned.getProperties());
		if (listener == null || !listener.isReturnListener()) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("No Listener for returned message");
			}
		}
		else {
			if (confirm != null) {
				confirm.setReturned(true);
			}
			Listener listenerToInvoke = listener;
			PendingConfirm toCountDown = confirm;
			this.executor.execute(() -> {
				try {
					listenerToInvoke.handleReturn(returned);
				}
				catch (Exception e) {
					this.logger.error("Exception delivering returned message ", e);
				}
				finally {
					if (toCountDown != null) {
						toCountDown.countDown();
					}
				}
			});
		}
	}

	@Nullable
	private PendingConfirm findConfirm(Return returned) {
		LongString returnCorrelation = (LongString) returned.getProperties().getHeaders()
				.get(RETURNED_MESSAGE_CORRELATION_KEY);
		PendingConfirm confirm = null;
		if (returnCorrelation != null) {
			confirm = this.pendingReturns.remove(returnCorrelation.toString());
			if (confirm != null) {
				MessageProperties messageProperties = CONVERTER.toMessageProperties(returned.getProperties(),
						new Envelope(0L, false, returned.getExchange(), returned.getRoutingKey()),
						StandardCharsets.UTF_8.name());
				if (confirm.getCorrelationData() != null) {
					confirm.getCorrelationData().setReturned(new ReturnedMessage(// NOSONAR never null
							new Message(returned.getBody(), messageProperties), returned.getReplyCode(),
							returned.getReplyText(), returned.getExchange(), returned.getRoutingKey()));
				}
			}
		}
		return confirm;
	}

	@Nullable
	private Listener findListener(AMQP.BasicProperties properties) {
		Listener listener = null;
		Object returnListenerHeader = properties.getHeaders().get(RETURN_LISTENER_CORRELATION_KEY);
		String uuidObject = null;
		if (returnListenerHeader != null) {
			uuidObject = returnListenerHeader.toString();
		}
		if (uuidObject != null) {
			listener = this.listeners.get(uuidObject);
		}
		else {
			this.logger.error("No '" + RETURN_LISTENER_CORRELATION_KEY + "' header in returned message");
		}
		return listener;
	}

// ShutdownListener

	@Override
	public void shutdownCompleted(ShutdownSignalException cause) {
		shutdownCompleted(cause.getMessage());
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

	public static PublisherCallbackChannelFactory factory() {
		return (channel, exec) -> new PublisherCallbackChannelImpl(channel, exec);
	}

}
