package continuations;

import java.util.Timer;
import java.util.TimerTask;

/**
 * A demonstration of how Continuations (Coroutines) are implemented purely in Java 
 * without OS threads or Virtual Threads.
 * 
 * This is fundamentally what bytecode manipulators (like Kilim) or languages 
 * (like Kotlin or C#'s await) do under the hood: 
 * They convert your linear-looking code into a State Machine class.
 */
public class StateMachineDemo {

    // 1. The core interface for a Continuation. 
    // It is just a callback that stores where it left off!
    public interface Continuation {
        void resumeWith(Object result);
    }

    // 2. A simulated asynchronous network service.
    // Notice that it takes a Continuation instead of blocking the thread.
    static class SimulatedNetwork {
        private final Timer timer = new Timer(true);

        public void sendQueryAsync(String queryId, Continuation cont) {
            System.out.println("   [Network] Sending Query... (thread will NOT block)");
            // Simulate network delay of 1 second
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("   [Network] Query response received!");
                    cont.resumeWith("QUERY_OK");
                }
            }, 1000);
        }

        public void sendAcceptAsync(String reqId, Continuation cont) {
            System.out.println("   [Network] Sending Accept... (thread will NOT block)");
            // Simulate network delay of 1 second
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("   [Network] Accept response received!");
                    cont.resumeWith("ACCEPT_OK");
                }
            }, 1000);
        }
    }

    // 3. The "Coroutined" method. 
    // Instead of nested callbacks, the compiler generates a State Machine.
    static class ThreePhaseStateMachine implements Continuation {
        // The JVM local variables are hoisted into class fields!
        int label = 0; // The instruction pointer
        String originalRequestId;
        String queryResponse;
        
        // Dependencies
        final SimulatedNetwork network;

        public ThreePhaseStateMachine(SimulatedNetwork network, String requestId) {
            this.network = network;
            this.originalRequestId = requestId;
        }

        // This is the "tick" or "resume" function
        @Override
        public void resumeWith(Object result) {
            switch (label) {
                case 0:
                    // --- PHASE 0 ---
                    System.out.println("[State 0] Starting Coordinator for " + originalRequestId);
                    
                    // Update state pointer BEFORE yielding
                    label = 1; 
                    
                    // Call async network, passing `this` as the continuation.
                    // This is the "yield"! Execution returns completely!
                    network.sendQueryAsync("qry_123", this); 
                    return;

                case 1:
                    // --- PHASE 1 ---
                    System.out.println("[State 1] Woke up! Resuming with Network Result: " + result);
                    queryResponse = (String) result; // Restore local variable
                    
                    // Update state pointer
                    label = 2;
                    
                    network.sendAcceptAsync("req_123", this);
                    return; // YIELD!

                case 2:
                    // --- PHASE 2 ---
                    System.out.println("[State 2] Woke up! Doing final commit.");
                    String acceptResponse = (String) result;
                    System.out.println("[State 2] Finished transaction: " + originalRequestId);
                    
                    label = -1; // Completed
                    return;
                    
                default:
                    throw new IllegalStateException("Coroutine already finished!");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Main Thread ID: " + Thread.currentThread().threadId());
        
        SimulatedNetwork network = new SimulatedNetwork();
        
        // 1. We instantiate the coroutine state machine
        ThreePhaseStateMachine coordinator = new ThreePhaseStateMachine(network, "client_req_99");
        
        // 2. Start the execution (initial call)
        coordinator.resumeWith(null);
        
        System.out.println("-> Main thread is totally free immediately after starting!");
        
        // Sleep the main thread just to keep the JVM alive while the Timer threads respond.
        // In Tickloom, this would be your `while(true) { node.tick(); }` loop!
        Thread.sleep(3000);
    }
}
