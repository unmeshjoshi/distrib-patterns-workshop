package replicate.quorumkv;

import com.tickloom.ProcessId;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.ListenableFuture;
import com.tickloom.messaging.*;
import com.tickloom.network.MessageCodec;
import com.tickloom.util.Clock;

import java.util.List;
import java.util.Map;

public class QuorumKVClient extends ClusterClient {
    
    public QuorumKVClient(ProcessId clientId, List<ProcessId> replicaEndpoints,
                               MessageBus messageBus, MessageCodec messageCodec,
                               Clock clock, int timeoutTicks) {
        super(clientId, replicaEndpoints, messageBus, messageCodec, clock, timeoutTicks);
    }
    
    public ListenableFuture<GetResponse> get(byte[] key) {
        GetRequest request = new GetRequest(key);
        ProcessId primaryReplica = firstReplica();
        
        return sendRequest(request, primaryReplica, QuorumKVMessageTypes.CLIENT_GET_REQUEST);
    }
    
    public ListenableFuture<SetResponse> set(byte[] key, byte[] value) {
        SetRequest request = new SetRequest(key, value);
        ProcessId primaryReplica = firstReplica();
        
        return sendRequest(request, primaryReplica, QuorumKVMessageTypes.CLIENT_SET_REQUEST);
    }


    private void handleSetResponse(Message message) {
        SetResponse response = deserialize(message.payload(), SetResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }

    private void handleGetResponse(Message message) {
        GetResponse response = deserialize(message.payload(), GetResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                QuorumKVMessageTypes.CLIENT_GET_RESPONSE, this::handleGetResponse,
                QuorumKVMessageTypes.CLIENT_SET_RESPONSE, this::handleSetResponse);

    }

    private ProcessId firstReplica() {
        return replicaEndpoints.get(0);
    }
}

