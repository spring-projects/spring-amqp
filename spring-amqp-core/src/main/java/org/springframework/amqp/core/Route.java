package org.springframework.amqp.core;

import org.springframework.amqp.core.Exchange;

/**
 * User: Benjamin Bennett benjamin.j.bennett@boeing.com
 * Date: Nov 21, 2010
 * Time: 8:35:07 PM
 */
public class Route {

	private Exchange exchange;

	private String routingKey;

	public Route(Route route){
		this.exchange = route.getExchange();
		this.routingKey = route.getRoutingKey();
	}
	public Route(Exchange exchange, String routingKey) {
		this.exchange = exchange;
		this.routingKey = routingKey;
	}

	public Exchange getExchange() {
		return exchange;
	}

	public void setExchange(Exchange exchange) {
		this.exchange = exchange;
	}

	public String getExchangeName() {
		return this.getExchange().getName();
	}

	public String getRoutingKey() {
		return routingKey;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}
}
