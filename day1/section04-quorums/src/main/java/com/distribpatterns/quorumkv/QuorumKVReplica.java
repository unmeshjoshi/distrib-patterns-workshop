package com.distribpatterns.quorumkv;

import com.distribpatterns.quorumkv.messages.*;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.AsyncQuorumCallback;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Quorum-based replica implementation for distributed key-value store.
 * This implementation uses majority quorum consensus for read and write operations.
 * <p>
 * Based on the quorum consensus algorithm where operations require majority
 * agreement from replicas before completing successfully.
 */
public class QuorumKVReplica extends Replica {

    private final boolean enableReadRepair;
    private final boolean asyncReadRepair;

    public QuorumKVReplica(List<ProcessId> peerIds, ProcessParams processParams) {
        this(peerIds, processParams, false, false);
    }

    public QuorumKVReplica(List<ProcessId> peerIds, ProcessParams processParams, boolean enableReadRepair, boolean asyncReadRepair) {
        super(peerIds, processParams);
        this.enableReadRepair = enableReadRepair;
        this.asyncReadRepair = asyncReadRepair;
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                QuorumKVMessageTypes.CLIENT_GET_REQUEST, this::handleClientGetRequest,
                QuorumKVMessageTypes.CLIENT_SET_REQUEST, this::handleClientSetRequest,
                QuorumKVMessageTypes.INTERNAL_GET_RESPONSE, this::handleInternalGetResponse,
                QuorumKVMessageTypes.INTERNAL_SET_RESPONSE, this::handleInternalSetResponse,
                QuorumKVMessageTypes.INTERNAL_GET_REQUEST, this::handleInternalGetRequest,
                QuorumKVMessageTypes.INTERNAL_SET_REQUEST, this::handleInternalSetRequest);
    }

    // Client request handlers

    private void handleClientGetRequest(Message message) {
        final var correlationId = message.correlationId();
        var clientRequest = deserializePayload(message.payload(), GetRequest.class);
        var clientId = message.source();

        logIncomingGetRequest(clientRequest, correlationId, clientId);

        var quorumCallback = createGetQuorumCallback();
        quorumCallback
                .onSuccess(responses -> handleSuccessfulGetRequest(clientRequest, correlationId, clientId, responses))
                .onFailure(
                        error -> sendFailureGetResponse(clientRequest, correlationId, clientId, error));

        broadcastInternalGetRequest(clientRequest, quorumCallback);
    }

    private void handleSuccessfulGetRequest(GetRequest request,
                                            String correlationId,
                                            ProcessId clientId,
                                            Map<ProcessId, InternalGetResponse> responses) {
        var latestValue = getLatestValue(responses);
        if (!shouldRepairRead(latestValue)) {
            sendSuccessGetResponse(request, correlationId, clientId, latestValue);
            return;
        }

        if (asyncReadRepair) {
            startAsyncReadRepair(request, responses);
            sendSuccessGetResponse(request, correlationId, clientId, latestValue);
            return;
        }

        startSynchronousReadRepair(request, responses, () -> sendSuccessGetResponse(request, correlationId, clientId, latestValue));
    }

    private boolean shouldRepairRead(VersionedValue latestValue) {
        return enableReadRepair && latestValue != null;
    }

    private void startAsyncReadRepair(GetRequest request, Map<ProcessId, InternalGetResponse> responses) {
        var repairCallback = createSetQuorumCallback();
        repairCallback
                .onSuccess(ignored -> { })
                .onFailure(ignored -> { });

        broadcastReadRepair(request, responses, repairCallback);
    }

    private void startSynchronousReadRepair(GetRequest request,
                                            Map<ProcessId, InternalGetResponse> responses,
                                            Runnable afterRepair) {
        var repairCallback = createSetQuorumCallback();
        repairCallback
                .onSuccess(ignored -> afterRepair.run())
                .onFailure(ignored -> afterRepair.run());

        broadcastReadRepair(request, responses, repairCallback);
    }

    private void broadcastReadRepair(GetRequest request,
                                     Map<ProcessId, InternalGetResponse> responses,
                                     AsyncQuorumCallback<InternalSetResponse> repairCallback) {
        var latestValue = getLatestValue(responses);
        broadcastInternalSetRequest(request.key(), latestValue, repairCallback);
    }

    private VersionedValue getLatestValue(Map<ProcessId, InternalGetResponse> responses) {
        return responses.values()
                .stream()
                .map(InternalGetResponse::value)
                .filter(value -> value != null)
                .map(value -> messageCodec.decode(value, VersionedValue.class))
                .max(Comparator.comparingLong(VersionedValue::timestamp))
                .orElse(null);
    }

    private AsyncQuorumCallback<InternalGetResponse> createGetQuorumCallback() {
        var allNodes = getAllNodes();
        return new AsyncQuorumCallback<>(
                allNodes.size(),
                response -> response != null   //null values is valid response. TODO:refactor.
        );
    }

    private void logIncomingGetRequest(GetRequest req, String correlationId, ProcessId clientAddr) {
        System.out.println("QuorumKVReplica: Processing client GET request - keyLength: " + req.key().length +
                ", correlationId: " + correlationId + ", from: " + clientAddr);
    }

    private void sendSuccessGetResponse(GetRequest req, String correlationId, ProcessId clientAddr,
                                        VersionedValue latestValue) {
        logSuccessfulGetResponse(req, correlationId, latestValue);
        sendGetResponseToClient(req, correlationId, clientAddr, latestValue);
    }

    private void sendGetResponseToClient(GetRequest req, String correlationId, ProcessId clientAddr, VersionedValue latestValue) {
        var response = new GetResponse(req.key(),
                latestValue != null ? latestValue.value() : null,
                latestValue != null);

        var responseMessage = createMessage(clientAddr, correlationId, response, QuorumKVMessageTypes.CLIENT_GET_RESPONSE);

        System.out.println("Sending client response for key = " + new String(req.key()) + " " + response);

        send(responseMessage);
    }

    private void sendFailureGetResponse(GetRequest req, String correlationId, ProcessId clientAddr, Throwable error) {
        logFailedGetResponse(req, correlationId, error);

        var response = new GetResponse(req.key(), null, false);

        var responseMessage = createMessage(clientAddr, correlationId, response, QuorumKVMessageTypes.CLIENT_GET_RESPONSE);

        System.out.println("Sending client response for key = " + new String(req.key()) + " " + response);
        send(responseMessage);
    }

    private void logSuccessfulGetResponse(GetRequest req, String correlationId, VersionedValue latestValue) {
        System.out.println("QuorumKVReplica: Successful GET response - key: " + new String(req.key()) +
                ", correlationId: " + correlationId + ", hasValue: " + (latestValue != null));
    }

    private void logFailedGetResponse(GetRequest req, String correlationId, Throwable error) {
        System.out.println("QuorumKVReplica: Failed GET response - key: " + new String(req.key()) +
                ", correlationId: " + correlationId + ", error: " + error.getMessage());
    }

    private void handleClientSetRequest(Message message) {
        var correlationId = message.correlationId();
        var clientRequest = deserializePayload(message.payload(), SetRequest.class);
        var clientAddress = message.source();

        logIncomingSetRequest(clientRequest, correlationId, clientAddress);

        var quorumCallback = createSetQuorumCallback();
        quorumCallback.onSuccess(responses -> sendSuccessSetResponseToClient(correlationId, clientAddress, clientRequest.key()))
                .onFailure(error -> sendFailureSetResponseToClient(correlationId, clientAddress, clientRequest.key()));

        var timestamp = clock.now(); //System.currentTimeMillis or TIME_OF_THE_DAY_CLOCK.
        var versionedValue = new VersionedValue(clientRequest.value(), timestamp);
        broadcastInternalSetRequest(clientRequest.key(), versionedValue, quorumCallback);
    }

    private void logIncomingSetRequest(SetRequest req, String correlationId, ProcessId clientAddr) {
        System.out.println("QuorumKVReplica: Processing client SET request - keyLength: " + req.key().length +
                ", valueLength: " + req.value().length + ", correlationId: " + correlationId + ", from: " + clientAddr);
    }

    private AsyncQuorumCallback<InternalSetResponse> createSetQuorumCallback() {
        var allNodes = getAllNodes();
        return new AsyncQuorumCallback<>(
                allNodes.size(),
                response -> response != null && response.success()
        );
    }

    private void sendSuccessSetResponseToClient(String correlationId, ProcessId clientAddr, byte[] key) {
        var response = new SetResponse(key, true);

        var responseMessage = createMessage(clientAddr, correlationId, response, QuorumKVMessageTypes.CLIENT_SET_RESPONSE);

        send(responseMessage);
    }

    private void sendFailureSetResponseToClient(String correlationId, ProcessId clientAddr, byte[] key) {
        var response = new SetResponse(key, false);

        var responseMessage = createMessage(clientAddr, correlationId, response, QuorumKVMessageTypes.CLIENT_SET_RESPONSE);

        send(responseMessage);
    }
    // Internal request handlers

    private void handleInternalGetRequest(Message message) {
        var getRequest = deserializePayload(message.payload(), InternalGetRequest.class);
        System.out.println("QuorumKVReplica: Processing internal GET request - keyLength: " + getRequest.key().length
                + ", from: " + message.source());
        // Perform local storage operation
        var future = storage.get(getRequest.key());
        future.whenComplete((value, error) -> {
            sendInternalGetResponse(message, value, error, getRequest);
        });
    }

    private void sendInternalGetResponse(Message incomingMessage, byte[] value, Throwable error, InternalGetRequest getRequest) {

        logInternalGetResponse(value, error, getRequest);
        //value will be null if not found or error
        var response = new InternalGetResponse(getRequest.key(), value);

        var responseMessage = createResponseMessage(incomingMessage, response, QuorumKVMessageTypes.INTERNAL_GET_RESPONSE);

        System.out.println("Sending response message for key = " + new String(getRequest.key()) + " " + responseMessage);

        send(responseMessage);

    }

    private static void logInternalGetResponse(byte[] value, Throwable error, InternalGetRequest getRequest) {
        if (error == null) {
            String valueStr = value != null ? value + "found" : "not found";
            System.out.println("QuorumKVReplica: Internal GET completed - key: " + getRequest.key() +
                    ", value: " + valueStr);

        } else {
            System.out.println("QuorumKVReplica: Internal GET failed - key: " + getRequest.key() +
                    ", error: " + error.getMessage());
        }
    }

    private void handleInternalSetRequest(Message message) {
        var setRequest = deserializePayload(message.payload(), InternalSetRequest.class);
        var value = new VersionedValue(setRequest.value(), setRequest.timestamp());

        System.out.println("QuorumReplica: Processing internal SET request - keyLength: " + setRequest.key().length +
                ", valueLength: " + setRequest.value().length + ", timestamp: " + setRequest.timestamp() +
                ", from: " + message.source());

        //First get the value and set only if it does not exist or its of lower timestamp.
        ListenableFuture<byte[]> getFuture = storage.get(setRequest.key());
        getFuture.whenComplete((result, error) -> {
            if (error != null) {
                sendInternalSetResponse(message, false, error, setRequest);
                return;
            }
            if (!shouldOverwriteExistingValue(result, value)) {
                //Already set with higher timestamp, so we return true, but dont overwrite the value.
                sendInternalSetResponse(message, true, null, setRequest);
                return;
            }
            storeVersionedValue(message, setRequest, value);
        });
    }

    private boolean shouldOverwriteExistingValue(byte[] existingEncodedValue, VersionedValue newValue) {
        if (existingEncodedValue == null) {
            return true;
        }

        VersionedValue existingValue = messageCodec.decode(existingEncodedValue, VersionedValue.class);
        return existingValue.timestamp() < newValue.timestamp();
    }

    private void storeVersionedValue(Message message, InternalSetRequest setRequest, VersionedValue value) {
        var future = storage.put(setRequest.key(), messageCodec.encode(value));
        future.whenComplete((success, setError)
                -> sendInternalSetResponse(message, success, setError, setRequest));
    }

    private void sendInternalSetResponse(Message message, Boolean success, Throwable error, InternalSetRequest setRequest) {
        logInternalSetResponse(success, error, setRequest);

        var response = new InternalSetResponse(setRequest.key(), success);
        //success will be false if error
        var responseMessage = createResponseMessage(message, response, QuorumKVMessageTypes.INTERNAL_SET_RESPONSE);

        send(responseMessage);

    }

    private static void logInternalSetResponse(Boolean success, Throwable error, InternalSetRequest setRequest) {
        if (error == null) {
            System.out.println("QuorumKVReplica: Internal SET completed - keyLength: " + setRequest.key().length +
                    ", success: " + success);

        } else {
            System.out.println("QuorumKVReplica: Internal SET failed - keyLength: " + setRequest.key().length +
                    ", error: " + error.getMessage());

        }
    }

    private void handleInternalGetResponse(Message message) {
        var response = deserializePayload(message.payload(), InternalGetResponse.class);

        System.out.println("QuorumKVReplica: Processing internal GET response - keyLength: " + response.key().length +
                ", from: " + message.source());

        // Route the response to the RequestWaitingList
        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    private void handleInternalSetResponse(Message message) {
        var response = deserializePayload(message.payload(), InternalSetResponse.class);

        System.out.println("QuorumKVReplica: Processing internal SET response - keyLength: " + response.key().length +
                ", success: " + response.success() + ", internalCorrelationId: " + message.correlationId() +
                ", from: " + message.source());

        waitingList.handleResponse(message.correlationId(), response, message.source());
    }

    private void broadcastInternalGetRequest(GetRequest request, AsyncQuorumCallback<InternalGetResponse> quorumCallback) {
        broadcastToAllReplicas(quorumCallback, (node, internalCorrelationId) -> {
            var internalRequest = new InternalGetRequest(request.key());
            return createMessage(node, internalCorrelationId, internalRequest, QuorumKVMessageTypes.INTERNAL_GET_REQUEST);
        });
    }

    private void broadcastInternalSetRequest(byte[] key,
                                             VersionedValue value,
                                             AsyncQuorumCallback<InternalSetResponse> quorumCallback) {
        broadcastToAllReplicas(quorumCallback, (node, internalCorrelationId) -> {
            var internalRequest = new InternalSetRequest(key, value.value(), value.timestamp());
            return createMessage(node, internalCorrelationId, internalRequest, QuorumKVMessageTypes.INTERNAL_SET_REQUEST);
        });
    }

}
