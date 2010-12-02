package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.FlowListener;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.ShutdownNotifierComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of RabbitMQ Channel that decorates the default implementation and provides a mechanism to recover the
 * channel in light of a failure. If the underlying channel is closed due to a communications failure, the underlying
 * channel can be recreated using the {@link HAChannelConfiguration} implementation set in
 * {@link #configureChannel(HAChannelConfiguration)} method. Any channel method can be called in the channel
 * configuration object, but it is the responsibility of the user to ensure that the channel configuration is consistent
 * and does not introduce any errors if a re-creation of the channel is required.
 */
public class HAChannel extends ShutdownNotifierComponent implements Channel, ShutdownListener {

    private static final int UNDEFINED = -1;

    private final HAConnection connection;
    private final Object recoveryMutex = new Object();

    private HAChannelConfiguration configuration;
    private List<HAChannelConfiguration> internalConfigurations = new ArrayList<HAChannelConfiguration>();
    private Channel channel;
    private int channelNumber = UNDEFINED;

    private static final Logger LOG = LoggerFactory.getLogger(HAChannel.class);

    /**
     * Create an HAChannel from the given connection.
     * @param connection The connection to use to create the channel.
     * @throws java.io.IOException If there is a problem creating the channel.
     */
    public HAChannel(HAConnection connection) throws IOException {
        this.connection = connection;
        createChannel();
    }

    /**
     * Create an HAChannel from the given connection.
     * @param connection The connection to use to create the channel.
     * @param channelNumber The channel number to use to create the channel.
     * @throws java.io.IOException If there us a problem creating the channel.
     */
    public HAChannel(HAConnection connection, int channelNumber) throws IOException {
        this.connection = connection;
        this.channelNumber = channelNumber;
        createChannel();
    }

    /**
     * Configure the channel using this configuration. The configuration object supplied will hereafter be used to
     * recreate the channel in the event of a failure. This configuration object does not inherently guarantee that
     * the recreation is faithful to the original channel. It is up to the user to ensure that the configurations used
     * maintain consistency in re-creation of the channel.
     * @param configuration The channel configuration to use both to immediately configure the channel and to re-create
     * it in the event of a connection failure.
     * @throws java.io.IOException if the channel cannot be configured.
     */
    public void configureChannel(HAChannelConfiguration configuration) throws IOException {
        this.configuration = configuration;
        configureChannel();
    }

    private void createChannel() throws IOException {
        if(channelNumber == UNDEFINED){
            channel = connection.createUnderlyingChannel();
        } else {
            channel = connection.createUnderlyingChannel(channelNumber);
            if(channel == null){
                throw new IllegalArgumentException("Channel number: " + channelNumber + " already in use.");
            }
        }
    }

    private void configureChannel() throws IOException{
        if(configuration != null){
            configuration.configureChannel(getUnderlyingChannel());
        }
        for(HAChannelConfiguration internalConfiguration : internalConfigurations){
            internalConfiguration.configureChannel(getUnderlyingChannel());
        }
    }

    /**
     * This method is to be used by the HA classes to add their own configurations independently of the developer's own
     * configurations. Currently used by the HARpcServer to reattach their consumers.
     * @param configuration The configuration to use.
     * @throws java.io.IOException If there is a problem with the configuration.
     */
    void addInternalConfiguration (HAChannelConfiguration configuration) throws IOException {
        configuration.configureChannel(getUnderlyingChannel());
        internalConfigurations.add(configuration);
    }

    public void shutdownCompleted(ShutdownSignalException cause) {
        if(cause.isInitiatedByApplication()){
            LOG.info(this.getClass().toString() + ": HAChannel closed because: ", cause);
            this._shutdownCause = cause;
            this.notifyListeners();
            connection.unregisterChannel(this);
        } else {
            LOG.info(this.getClass().toString() + ": Underlying channel invalidated because: ", cause);
        }
    }

    public int getChannelNumber() {
        return getUnderlyingChannel().getChannelNumber();
    }

    public Connection getConnection() {
        return connection;
    }

    public void close() throws IOException {
        synchronized(recoveryMutex){
            if(channel.isOpen()){
                channel.close();
            } else { //underlying channel already closed perhaps as a result of server shutdown.
                channel.abort();
            }
        }
    }

    public void close(int closeCode, String closeMessage) throws IOException {
        synchronized(recoveryMutex){
            if(channel.isOpen()){
                channel.close(closeCode, closeMessage);
            } else { //underlying channel already closed perhaps as a result of server shutdown.
                channel.abort(closeCode, closeMessage);
            }
        }
    }

    public AMQP.Channel.FlowOk flow(boolean active) throws IOException {
        return getUnderlyingChannel().flow(active);
    }

    public AMQP.Channel.FlowOk getFlow() {
        return getUnderlyingChannel().getFlow();
    }

    public void abort() throws IOException {
        getUnderlyingChannel().abort();
    }

    public void abort(int closeCode, String closeMessage) throws IOException {
        getUnderlyingChannel().abort(closeCode, closeMessage);
    }

    public ReturnListener getReturnListener() {
        return getUnderlyingChannel().getReturnListener();
    }

    public void setReturnListener(ReturnListener listener) {
        getUnderlyingChannel().setReturnListener(listener);
    }

    public FlowListener getFlowListener() {
        return getUnderlyingChannel().getFlowListener();
    }

    public void setFlowListener(FlowListener listener) {
        getUnderlyingChannel().setFlowListener(listener);
    }

    public Consumer getDefaultConsumer() {
        return getUnderlyingChannel().getDefaultConsumer();
    }

    public void setDefaultConsumer(Consumer consumer) {
        getUnderlyingChannel().setDefaultConsumer(consumer);
    }

    public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
        getUnderlyingChannel().basicQos(prefetchSize, prefetchCount, global);
    }

    public void basicQos(int prefetchCount) throws IOException {
        getUnderlyingChannel().basicQos(prefetchCount);
    }

    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
        getUnderlyingChannel().basicPublish(exchange, routingKey, props, body);
    }

    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) throws IOException {
        getUnderlyingChannel().basicPublish(exchange, routingKey, mandatory, immediate, props, body);
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
        return getUnderlyingChannel().exchangeDeclare(exchange, type);
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
        return getUnderlyingChannel().exchangeDeclare(exchange, type, durable);
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return getUnderlyingChannel().exchangeDeclare(exchange, type, durable, autoDelete, arguments);
    }

    public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException {
        return getUnderlyingChannel().exchangeDeclarePassive(name);
    }

    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
        return getUnderlyingChannel().exchangeDelete(exchange, ifUnused);
    }

    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
        return getUnderlyingChannel().exchangeDelete(exchange);
    }

    public AMQP.Queue.DeclareOk queueDeclare() throws IOException {
        return getUnderlyingChannel().queueDeclare();
    }

    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return getUnderlyingChannel().queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }

    public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
        return getUnderlyingChannel().queueDeclarePassive(queue);
    }

    public AMQP.Queue.DeleteOk queueDelete(String queue) throws IOException {
        return getUnderlyingChannel().queueDelete(queue);
    }

    public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
        return getUnderlyingChannel().queueDelete(queue, ifUnused, ifEmpty);
    }

    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) throws IOException {
        return getUnderlyingChannel().queueBind(queue, exchange, routingKey);
    }

    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
        return getUnderlyingChannel().queueBind(queue, exchange, routingKey, arguments);
    }

    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) throws IOException {
        return getUnderlyingChannel().queueUnbind(queue, exchange, routingKey);
    }

    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
        return getUnderlyingChannel().queueUnbind(queue, exchange, routingKey, arguments);
    }

    public AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException {
        return getUnderlyingChannel().queuePurge(queue);
    }

    public GetResponse basicGet(String queue, boolean noAck) throws IOException {
        return getUnderlyingChannel().basicGet(queue, noAck);
    }

    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        getUnderlyingChannel().basicAck(deliveryTag, multiple);
    }

    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
        getUnderlyingChannel().basicReject(deliveryTag, requeue);
    }

    public String basicConsume(String queue, Consumer callback) throws IOException {
        return getUnderlyingChannel().basicConsume(queue, callback);
    }

    public String basicConsume(String queue, boolean noAck, Consumer callback) throws IOException {
        return getUnderlyingChannel().basicConsume(queue, noAck, callback);
    }

    public String basicConsume(String queue, boolean noAck, String consumerTag, Consumer callback) throws IOException {
        return getUnderlyingChannel().basicConsume(queue, noAck, consumerTag, callback);
    }

    public String basicConsume(String queue, boolean noAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
        return getUnderlyingChannel().basicConsume(queue, noAck, consumerTag, noLocal, exclusive, arguments, callback);
    }

    public void basicCancel(String consumerTag) throws IOException {
        getUnderlyingChannel().basicCancel(consumerTag);
    }

    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) throws IOException {
        return getUnderlyingChannel().basicRecover(requeue);
    }

    @Deprecated
    public void basicRecoverAsync(boolean requeue) throws IOException {
        getUnderlyingChannel().basicRecoverAsync(requeue);
    }

    public AMQP.Tx.SelectOk txSelect() throws IOException {
        return getUnderlyingChannel().txSelect();
    }

    public AMQP.Tx.CommitOk txCommit() throws IOException {
        return getUnderlyingChannel().txCommit();
    }

    public AMQP.Tx.RollbackOk txRollback() throws IOException {
        return getUnderlyingChannel().txRollback();
    }

    /**
     * HA client API method to allow the re-creation of a channel in the event of a connection problem.
     * @throws java.io.IOException If there is a problem re-creating the channel.
     */
    void recreateChannel() throws IOException {
        synchronized(recoveryMutex){
            channel.abort();
            createChannel();
            configureChannel();
        }
    }


    /**
     * Get access to a consistent view of the underlying channel. Also used in testing.
     * @return the current underlying channel.
     */
    Channel getUnderlyingChannel() {
        synchronized (recoveryMutex){ //ensure that a consistent view is always seen of the underlying channel.
            return channel;
        }
    }

    /**
     * Remove a redundant internal configuration.
     * @param channelConfiguration the internal configuration to remove.
     */
    void removeInternalConfiguration(HAChannelConfiguration channelConfiguration) {
        internalConfigurations.remove(channelConfiguration);
    }
}
