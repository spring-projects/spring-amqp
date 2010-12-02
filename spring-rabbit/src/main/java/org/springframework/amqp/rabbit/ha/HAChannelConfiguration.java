package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.Channel;

import java.io.IOException;

/**
 * This interface defines the responsibility of classes that are intended to configure channels in an HA environment.
 * Instances of this interface are intended to be used whenever the underlying channel is created or re-created. In an
 * ideal world the configuration is used only once, but if something goes wrong with the connection then the underlying
 * channel can be re-created and the configuration re-applied. In use cases where a channel is re-configured from time
 * to time then all re-configuration in an HA environment will need to be done by instances of this interface.
 * @see HAChannel#configureChannel(HAChannelConfiguration)
 */
public interface HAChannelConfiguration {

    /**
     * Configure the channel.
     * @param channel the channel to be configured.
     * @throws java.io.IOException if there is a problem in the configuration.
     */
    void configureChannel(Channel channel) throws IOException;
}
