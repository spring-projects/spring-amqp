/**
 *
 */
package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.AmqpException;

import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 *
 */
public interface Connection {

    /**
     * Create a new channel, using an internally allocated channel number.
     * @param transactional true if the channel should support transactions
     * @return a new channel descriptor, or null if none is available
     * @throws AmqpException if an I/O problem is encountered
     */
    Channel createChannel(boolean transactional) throws AmqpException;

    /**
     * Close this connection and all its channels
     * with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
     * and message 'OK'.
     *
     * Waits for all the close operations to complete.
     *
     * @throws AmqpException if an I/O problem is encountered
     */
    void close() throws AmqpException;

    /**
     * Flag to indicate the status of the connection.
     *
     * @return true if the connection is open
     */
    boolean isOpen();

}
