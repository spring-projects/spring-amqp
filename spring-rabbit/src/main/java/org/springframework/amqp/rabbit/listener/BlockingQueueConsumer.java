package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.utility.Utility;
/**
 * Variation on QueueingConsumer in RabbitMQ, uses 'put' instead of 'add' and stored a reference to the consumerTag that
 * was returned when this Consumer was registered with the channel so as to make it easy to close the consumer when shutting down.
 * @author Mark Pollack
 *
 */
public class BlockingQueueConsumer extends DefaultConsumer {

		private String _consumerTag;
	
	    private final BlockingQueue<Delivery> _queue;

	    // When this is non-null the queue is in shutdown mode and nextDelivery should
	    // throw a shutdown signal exception.
	    private volatile ShutdownSignalException _shutdown;

	    // Marker object used to signal the queue is in shutdown mode. 
	    // It is only there to wake up consumers. The canonical representation
	    // of shutting down is the presence of _shutdown. 
	    // Invariant: This is never on _queue unless _shutdown != null.
	    private static final Delivery POISON = new Delivery(null, null, null);

	    public BlockingQueueConsumer(Channel ch) {
	        this(ch, new LinkedBlockingQueue<Delivery>());
	    }

	    public BlockingQueueConsumer(Channel ch, BlockingQueue<Delivery> q)
	    {
	        super(ch);
	        this._queue = q;
	    }
	    
	    

	    public String getConsumerTag() {
			return _consumerTag;
		}
	    
	    public void setConsumerTag(String consumerTag) 
	    {
	    	_consumerTag = consumerTag;
	    }
	    
		@Override public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
	        _shutdown = sig; 
	        try {
				_queue.put(POISON);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }

	    @Override public void handleDelivery(String consumerTag,
	                               Envelope envelope,
	                               AMQP.BasicProperties properties,
	                               byte[] body)
	        throws IOException
	    {
	    	//TODO do we want to pass on 'consumerTag'?
	        checkShutdown();
	        try {
				this._queue.put(new Delivery(envelope, properties, body));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }

	    /**
	     * Encapsulates an arbitrary message - simple "bean" holder structure.
	     */
	    public static class Delivery {
	        private final Envelope _envelope;
	        private final AMQP.BasicProperties _properties;
	        private final byte[] _body;

	        public Delivery(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
	            _envelope = envelope;
	            _properties = properties;
	            _body = body;
	        }

	        /**
	         * Retrieve the message envelope.
	         * @return the message envelope
	         */
	        public Envelope getEnvelope() {
	            return _envelope;
	        }

	        /**
	         * Retrieve the message properties.
	         * @return the message properties
	         */
	        public BasicProperties getProperties() {
	            return _properties;
	        }

	        /**
	         * Retrieve the message body.
	         * @return the message body
	         */
	        public byte[] getBody() {
	            return _body;
	        }
	    }

	    /**
	     * Check if we are in shutdown mode and if so throw an exception.
	     */
	    private void checkShutdown(){
	      if(_shutdown != null) throw Utility.fixStackTrace(_shutdown);
	    }

	    /**
	     * If this is a non-POISON non-null delivery simply return it.
	     * If this is POISON we are in shutdown mode, throw _shutdown
	     * If this is null, we may be in shutdown mode. Check and see.
	     * @throws InterruptedException 
	     */
	    private Delivery handle(Delivery delivery) throws InterruptedException
	    {
	      if(delivery == POISON || (delivery == null && _shutdown != null)){
	        if(delivery == POISON) _queue.put(POISON);
	        throw Utility.fixStackTrace(_shutdown);
	      }
	      return delivery;
	    }

	    /**
	     * Main application-side API: wait for the next message delivery and return it.
	     * @return the next message
	     * @throws InterruptedException if an interrupt is received while waiting
	     * @throws ShutdownSignalException if the connection is shut down while waiting
	     */
	    public Delivery nextDelivery()
	        throws InterruptedException, ShutdownSignalException
	    {
	        return handle(_queue.take());
	    }

	    /**
	     * Main application-side API: wait for the next message delivery and return it.
	     * @param timeout timeout in millisecond
	     * @return the next message or null if timed out
	     * @throws InterruptedException if an interrupt is received while waiting
	     * @throws ShutdownSignalException if the connection is shut down while waiting
	     */
	    public Delivery nextDelivery(long timeout)
	        throws InterruptedException, ShutdownSignalException
	    {
	        checkShutdown();
	        return handle(_queue.poll(timeout, TimeUnit.MILLISECONDS));
	    }
	
}
