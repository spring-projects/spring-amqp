package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;


/**
 * High availability extension to the RabbitMQ connection factory. All connections produced by this factory
 * will attempt re-connection if any problem occurs communicating with the server. It is recommended that the
 * heartbeat feature be used to ensure that all connection problems be detected and recovered from.
 */
public class HAConnectionFactory extends ConnectionFactory {

    private final Executor recoveryExecutor;
    private final int maxRecoveryAttempts;
    private final long recoveryDelay;
    private static final Logger LOG = LoggerFactory.getLogger(HAConnectionFactory.class);

    /**
     * Constructor for an HAConnection factory.
     * @param recoveryExecutor The executor that schedules the recovery attempts. It is recommended that this executor
     * be created with a work queue no smaller than the number of connections that will need to be supported as they may
     * all need recovery at the same time.
     * @param maxRecoveryAttempts The maximum number of attempts before recovery should be abandoned.
     * @param recoveryDelay The length of time to delay between recovery attmpts in milliseconds. Be warned that this
     * delay currently sleeps the thread involved.
     */
    public HAConnectionFactory(Executor recoveryExecutor, int maxRecoveryAttempts, long recoveryDelay) {
        this.recoveryExecutor = recoveryExecutor;
        this.maxRecoveryAttempts = maxRecoveryAttempts;
        this.recoveryDelay = recoveryDelay;
    }

    @Override
    public HAConnection newConnection(Address[] addrs) throws IOException {
        return new HAConnection(addrs, this);
    }

    /**
     * Method used within the package to create the underlying (wrapped) connection instances.
     * @param addrs The host addresses to use in the search for a connection.
     * @return A properlyu established connection.
     * @throws java.io.IOException If there is a problem establishing the connection.
     */
    Connection createUnderlyingConnection(Address[] addrs) throws IOException {
        return super.newConnection(addrs);
    }

    /**
     * Important package method for managing the recovery of a connection. Uses the Executor to schedule the recovery
     * efforts.
     * @param haConnection The HAConnection to be recovered.
     * @param recoveryCount The number of times that the recovery has been attempted.
     */
    void recoverConnection(final HAConnection haConnection, final int recoveryCount) {
        recoveryExecutor.execute(new Runnable(){
            public void run(){
                try {
                    Thread.sleep(recoveryDelay);
                    haConnection.recoverConnection();
                } catch (Exception e) {
                    LOG.error("Failed to recover connection.", e);
                    if(recoveryCount > maxRecoveryAttempts){
                        recoverConnection(haConnection, recoveryCount + 1);
                    }
                }
            }
        });
    }
}
