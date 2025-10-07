package com.distribpatterns.tickloomexample.echo;

import com.tickloom.messaging.MessageType;

public final class EchoMessages {
    private EchoMessages() {}

    public static final MessageType ECHO_REQUEST = MessageType.of("ECHO_REQUEST");
    public static final MessageType ECHO_RESPONSE = MessageType.of("ECHO_RESPONSE");

    public record EchoRequest(String text) {}
    public record EchoResponse(String text) {}
}



