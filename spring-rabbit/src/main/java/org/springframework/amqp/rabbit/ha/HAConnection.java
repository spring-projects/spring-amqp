package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.ShutdownNotifierComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A connection that will auto-recover if there is an interruption between the client and the server.
 * This auto recovery includes re-creating any registered channels. This implementation does not attempt
 * to shield calling methods from any transient effects of the failure and will merrily throw exceptions
 * until the underlying connection has been successfully re-established.
 */
public class HAConnection extends ShutdownNotifierComponent implements Connection, ShutdownListener {
    private Connection connection;
    private final HAConnectionFactory haConnectionFactory;
    private final Address[] addrs;
    private final Set<HAChannel> channels = new HashSet<HAChannel>();
    private final Object recoveryMutex = new Object();
    private static final Logger LOG = LoggerFactory.getLogger(HAConnection.class);

    /**
     * Create an instance of HAConnection. It is more normal to use the connection factory to get an instance of this
     * class.
     * @param addrs The addresses to use when attempting to make the connection.
     * @param haConnectionFactory The connection factory to use with this HA connection.
     * @throws java.io.IOException if there is a problem with making the connection.
     */

    public HAConnection(Address[] addrs, HAConnectionFactory haConnectionFactory) throws IOException {
        this.addrs = addrs;
        this.haConnectionFactory = haConnectionFactory;
        establishConnection();
    }

    private void establishConnection() throws IOException {
        connection = haConnectionFactory.createUnderlyingConnection(addrs);
        connection.addShutdownListener(this);
    }

    public String getHost() {
        return getUnderlyingConnection().getHost();
    }

    public int getPort() {
        return getUnderlyingConnection().getPort();
    }

    public int getChannelMax() {
        return getUnderlyingConnection().getChannelMax();
    }

    public int getFrameMax() {
        return getUnderlyingConnection().getFrameMax();
    }

    public int getHeartbeat() {
        return getUnderlyingConnection().getHeartbeat();
    }

    public Map<String, Object> getClientProperties() {
        return getUnderlyingConnection().getClientProperties();
    }

    public Map<String, Object> getServerProperties() {
        return getUnderlyingConnection().getServerProperties();
    }

    public HAChannel createChannel() throws IOException {
        synchronized (recoveryMutex) {
            HAChannel haChannel = new HAChannel(this);
            channels.add(haChannel);
            return haChannel;
        }
    }

    public HAChannel createChannel(int channelNumber) throws IOException {
        synchronized (recoveryMutex) {
            HAChannel haChannel = new HAChannel(this, channelNumber);
            channels.add(haChannel);
            return haChannel;
        }
    }

    Channel createUnderlyingChannel(int channelNumber) throws IOException {
        return getUnderlyingConnection().createChannel(channelNumber);
    }

    public void close() throws IOException {
        getUnderlyingConnection().close();
    }

    public void close(int closeCode, String closeMessage) throws IOException {
        getUnderlyingConnection().close(closeCode, closeMessage);
    }

    public void close(int timeout) throws IOException {
        getUnderlyingConnection().close(timeout);
    }

    public void close(int closeCode, String closeMessage, int timeout) throws IOException {
        getUnderlyingConnection().close(closeCode, closeMessage, timeout);
    }

    public void abort() {
        getUnderlyingConnection().abort();
    }

    public void abort(int closeCode, String closeMessage) {
        getUnderlyingConnection().abort(closeCode, closeMessage);
    }

    public void abort(int timeout) {
        getUnderlyingConnection().abort(timeout);
    }

    public void abort(int closeCode, String closeMessage, int timeout) {
        getUnderlyingConnection().abort(closeCode, closeMessage, timeout);
    }

    public void shutdownCompleted(ShutdownSignalException cause) {
        if (cause.isInitiatedByApplication()) {
            LOG.info(this.getClass().toString() + ": HAConnection closed because: ", cause);
            _shutdownCause = cause;
            this.notifyListeners();
        } else {
            LOG.info(this.getClass().toString() + ": Underlying connection invalidated because: ", cause);
            haConnectionFactory.recoverConnection(this, 1);
        }
    }

    /**
     * Method to be used only within the package that attempts recovery.
     * @throws java.io.IOException If there is a problem in the recovery.
     */
    void recoverConnection() throws IOException {
        synchronized (recoveryMutex) {
            LOG.info("HAConnection recovery requested.");
            if (this.isOpen()) { // The HA connection has not been shut down so attempt reconnection.
                LOG.info("HAConnection recovery in progress.");
                connection.removeShutdownListener(this);
                connection.abort(); // close down old connection if necessary.
                establishConnection();
                for (HAChannel channel : channels) {
                    channel.recreateChannel();
                }
            }
        }
    }

    /**
     * Method to be used within the package to create the channel instances that will be wrapped. Used both at
     * initialisation and recovery.
     * @return the underlying channel instance.
     * @throws java.io.IOException if there is a problem with creating the channel.
     */
    Channel createUnderlyingChannel() throws IOException {
        return getUnderlyingConnection().createChannel();
    }

    /**
     * When a channel is closed it needs to be unregistered so that the HA API does not try to recreate it.
     * @param haChannel the channel to be unregistered.
     */
    void unregisterChannel(HAChannel haChannel) {
        synchronized(recoveryMutex){
            channels.remove(haChannel);
        }
    }

    /**
     * This method is used to provide a consistent view of the wrapped connection. During recovery the old connection
     * may still be in use and throw exceptions. Also used in testing.
     * @return The underlying (wrapped) connection.
     */
    Connection getUnderlyingConnection() {
        synchronized(recoveryMutex){ //ensure that a consistent view of the connection is always seen.
            return connection;
        }
    }
}
