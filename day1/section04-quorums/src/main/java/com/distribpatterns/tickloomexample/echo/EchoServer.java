package com.distribpatterns.tickloomexample.echo;

import com.tickloom.Process;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.network.MessageCodec;
import com.tickloom.storage.Storage;
import com.tickloom.util.Clock;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.distribpatterns.tickloomexample.echo.EchoMessages.*;

/**
 * A minimal implementation demonstrating the tickloom usage.
 * Each server side node and client extends Process.
 * They implement handlers for messages received from other nodes.
 * The communication between nodes, server to server or client to server
 * is done by message passing and not RPC.
 */
public class EchoServer extends Process {

    private final List<ProcessId> peerIds;

    public EchoServer(List<ProcessId> peerIds,
                      ProcessParams processParams) {
        super(processParams);
        this.peerIds = peerIds;
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                ECHO_REQUEST, this::onEchoRequest
        );
    }

    private void onEchoRequest(Message msg) {
        EchoRequest request = deserializePayload(msg.payload(), EchoRequest.class);
        EchoResponse response = new EchoResponse(request.text());
        Message responseMessage = createResponseMessage(msg, response, ECHO_RESPONSE);
        try {
            messageBus.sendMessage(responseMessage);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}



