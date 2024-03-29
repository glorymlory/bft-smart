/*
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.tom.core;

import bftsmart.consensus.Consensus;
import bftsmart.consensus.Decision;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.util.BatchReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class implements a thread which will deliver totally ordered requests to
 * the application
 */
public final class DeliveryThread extends Thread {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private boolean doWork = true;
    private int lastReconfig = -2;
    private final LinkedBlockingQueue<Decision> decided;
    private final TOMLayer tomLayer; // TOM layer
    private final ServiceReplica receiver; // Object that receives requests from clients
    private final Recoverable recoverer; // Object that uses state transfer
    private final ServerViewController controller;
    private final Lock decidedLock = new ReentrantLock();
    private final Condition notEmptyQueue = decidedLock.newCondition();

    //Variables used to pause/resume decisions delivery
    private final Lock pausingDeliveryLock = new ReentrantLock();
    private final Condition deliveryPausedCondition = pausingDeliveryLock.newCondition();
    private PriorityBlockingQueue<Decision> outOfSequenceValuesForDelivery;

    //
    private final Lock pausingDeliveryForSequentialPipeliningLock = new ReentrantLock();
    private int isPauseDelivery;

    /**
     * Creates a new instance of DeliveryThread
     *
     * @param tomLayer TOM layer
     * @param receiver Object that receives requests from clients
     */
    public DeliveryThread(TOMLayer tomLayer, ServiceReplica receiver, Recoverable recoverer,
                          ServerViewController controller) {
        super("Delivery Thread");
        this.decided = new LinkedBlockingQueue<>();
//		TODO ist it ok to init it with 10k max size?
        // comparator in acsending order

        this.outOfSequenceValuesForDelivery = new PriorityBlockingQueue<>(10000, (o1, o2) -> {
            if (o1.getConsensusId() == o2.getConsensusId()) {
                return 0;
            } else if (o1.getConsensusId() > o2.getConsensusId()) {
                return 1;
            } else {
                return -1;
            }
        });

        this.tomLayer = tomLayer;
        this.receiver = receiver;
        this.recoverer = recoverer;
        // ******* EDUARDO BEGIN **************//
        this.controller = controller;
        // ******* EDUARDO END **************//
    }

    public Recoverable getRecoverer() {
        return recoverer;
    }

    /**
     * Invoked by the TOM layer, to deliver a decision
     *
     * @param dec Decision established from the consensus
     */
    public void delivery(Decision dec) {

//		Not sequential pipelining case. Adding to out of sequence values for delivery
        if (dec.getConsensusId() > tomLayer.getLastExec() + 1) {
            logger.debug("Last finished consensus: {}, received DECIDED consensus to deliver: {}", tomLayer.getLastExec(), dec.getConsensusId());
            logger.info("Could not insert decision into decided queue, because value {} is out of sequence. Adding to out of sequence values for delivery", dec.getConsensusId());

            for(Decision decision : outOfSequenceValuesForDelivery) {
                logger.debug("Current out of sequence values for delivery: {}", decision.getConsensusId());
            }
            outOfSequenceValuesForDelivery.add(dec);
        } else {
            try {
                decidedLock.lock();
                decided.put(dec);
                // clean the ordered messages from the pending buffer
                TOMMessage[] requests = extractMessagesFromDecision(dec);
                tomLayer.clientsManager.requestsOrdered(requests);
                notEmptyQueue.signalAll();
                decidedLock.unlock();
                logger.debug("Consensus " + dec.getConsensusId() + " finished. Decided size=" + decided.size());
                logger.info("====Consensus {} finished=====", dec.getConsensusId());
            } catch (Exception e) {
                logger.error("Could not insert decision into decided queue", e);
            }

            if (!containsReconfig(dec)) {

                logger.debug("Decision from consensus " + dec.getConsensusId() + " does not contain reconfiguration");
                tomLayer.setLastExecAndRemoveInExec(dec.getConsensusId());
                if (!outOfSequenceValuesForDelivery.isEmpty()) {
                    processOutOfSequencePipelineDecision();
                }
            } // else if (tomLayer.controller.getStaticConf().getProcessId() == 0)
            // System.exit(0);
            else {
                logger.debug("Decision from consensus " + dec.getConsensusId() + " has reconfiguration");
                lastReconfig = dec.getConsensusId();

//                TODO check we add a replica not remove it.
//                reconfiguration request. Set pipeline to reconfiguration mode
                this.tomLayer.pipelineManager.setPipelineInReconfigurationMode(lastReconfig);
            }
        }
    }

    /**
     * Processes out-of-sequence pipeline decisions. If the consensus ID of the decision
     * matches the expected next decision, the out-of-sequence decision is delivered.
     */
    private void processOutOfSequencePipelineDecision() {
        Decision decision = outOfSequenceValuesForDelivery.peek();

        logger.debug("Processing out of sequence value  {}", decision.getConsensusId());
        if (decision.getConsensusId() == tomLayer.getLastExec() + 1) {
            outOfSequenceValuesForDelivery.poll();
            delivery(decision);
        }
    }

    /**
     * Cleaning up during synchronization phase.
     * Cleans up out-of-sequence values for delivery by resetting the associated requests
     * to not proposed and removing not delivered consensus. Also, clears the
     * outOfSequenceValuesForDelivery queue and cleans up consensuses in execution.
     */
    public void cleanUpOutOfSequenceValuesForDelivery() {

        for(Decision decision: outOfSequenceValuesForDelivery) {
            logger.debug("Cleaning up out of sequence values for delivery: {}", decision.getConsensusId());
            tomLayer.clientsManager.resetRequestsToNotProposed(decision.getDeserializedValue());
            this.tomLayer.execManager.removeNotDeliveredConsensus(decision.getConsensusId());
        }

        for (Integer cid: tomLayer.pipelineManager.getConsensusesInExecution()) {
            this.tomLayer.execManager.removeNotDeliveredConsensus(cid);
        }

        this.outOfSequenceValuesForDelivery.clear();
        tomLayer.pipelineManager.cleanUpConsensusesInExec();
        logger.debug("Cleaned up out of sequence values for delivery");
    }

    public List<TOMMessage> getOutOfSequenceValuesForDelivery() {
        List<TOMMessage> outOfSequenceValues = new ArrayList<>();
        for(Decision decision: outOfSequenceValuesForDelivery) {
            outOfSequenceValues.addAll(Arrays.asList(decision.getDeserializedValue()));
        }
        return outOfSequenceValues;
    }

    public int getLastOutOfSequenceValueForDelivery() {
        if (outOfSequenceValuesForDelivery.isEmpty()) {
            return -1;
        }
        return outOfSequenceValuesForDelivery.peek().getConsensusId();
    }

    private boolean containsReconfig(Decision dec) {
        TOMMessage[] decidedMessages = dec.getDeserializedValue();

        for (TOMMessage decidedMessage : decidedMessages) {
            if (decidedMessage.getReqType() == TOMMessageType.RECONFIG
                    && decidedMessage.getViewID() == controller.getCurrentViewId()) {
                return true;
            }
        }
        return false;
    }

    /**
     * THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER
     */
    private final ReentrantLock deliverLock = new ReentrantLock();
    private final Condition canDeliver = deliverLock.newCondition();


    /**
     * @deprecated This method does not always work when the replica was already delivering decisions.
     * This method is replaced by {@link #pauseDecisionDelivery()}.
     * Internally, the current implementation of this method uses {@link #pauseDecisionDelivery()}.
     */
    @Deprecated
    public void deliverLock() {
        pauseDecisionDelivery();
    }

    /**
     * @deprecated Replaced by {@link #resumeDecisionDelivery()} to work in pair with {@link #pauseDecisionDelivery()}.
     * Internally, the current implementation of this method calls {@link #resumeDecisionDelivery()}
     */
    @Deprecated
    public void deliverUnlock() {
        resumeDecisionDelivery();
    }

    /**
     * Pause the decision delivery.
     */
    public void pauseDecisionDelivery() {
        pausingDeliveryLock.lock();
        isPauseDelivery++;
        pausingDeliveryLock.unlock();

        // release the delivery lock to avoid blocking on state transfer
        decidedLock.lock();

        notEmptyQueue.signalAll();
        decidedLock.unlock();

        deliverLock.lock();
    }

    public void resumeDecisionDelivery() {
        pausingDeliveryLock.lock();
        if (isPauseDelivery > 0) {
            isPauseDelivery--;
        }
        if (isPauseDelivery == 0) {
            deliveryPausedCondition.signalAll();
        }
        pausingDeliveryLock.unlock();
        deliverLock.unlock();
    }

    /**
     * This method is used to restart the decision delivery after awaiting a state.
     */
    public void canDeliver() {
        canDeliver.signalAll();
    }

    public void update(ApplicationState state) {

        int lastCID = recoverer.setState(state);

        // set this decision as the last one from this replica
        logger.info("Setting last CID to " + lastCID);
        tomLayer.setLastExec(lastCID);

        // define the last stable consensus... the stable consensus can
        // be removed from the leaderManager and the executionManager
        if (lastCID > 2) {
            int stableConsensus = lastCID - 3;
            tomLayer.execManager.removeOutOfContexts(stableConsensus);
        }

        // define that end of this execution
        // stateManager.setWaiting(-1);
        tomLayer.setNoExec();

        logger.info("Current decided size: " + decided.size());
        decided.clear();

        logger.info("All finished up to " + lastCID);
    }

    /**
     * This is the code for the thread. It delivers decisions to the TOM request
     * receiver object (which is the application)
     */
    @Override
    public void run() {
        boolean init = true;
        while (doWork) {
            pausingDeliveryLock.lock();
            while (isPauseDelivery > 0) {
                deliveryPausedCondition.awaitUninterruptibly();
            }
            pausingDeliveryLock.unlock();
            deliverLock.lock();

            /* THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER */
            //deliverLock();
            while (tomLayer.isRetrievingState()) {
                logger.info("Retrieving State");
                canDeliver.awaitUninterruptibly();

                // if (tomLayer.getLastExec() == -1)
                if (init) {
                    logger.info(
                            "\n\t\t###################################"
                                    + "\n\t\t    Ready to process operations    "
                                    + "\n\t\t###################################");
                    init = false;
                }
            }

            try {
                ArrayList<Decision> decisions = new ArrayList<>();
                decidedLock.lock();
                if (decided.isEmpty()) {
                    notEmptyQueue.await();
                }

                logger.debug("Current size of the decided queue: {}", decided.size());

                if (controller.getStaticConf().getSameBatchSize()) {
                    decided.drainTo(decisions, 1);
                } else {
                    decided.drainTo(decisions);
                }

                decidedLock.unlock();

                if (!doWork)
                    break;

                if (decisions.size() > 0) {
                    TOMMessage[][] requests = new TOMMessage[decisions.size()][];
                    int[] consensusIds = new int[requests.length];
                    int[] leadersIds = new int[requests.length];
                    int[] regenciesIds = new int[requests.length];
                    CertifiedDecision[] cDecs;
                    cDecs = new CertifiedDecision[requests.length];
                    int count = 0;
                    for (Decision d : decisions) {
                        requests[count] = extractMessagesFromDecision(d);
                        consensusIds[count] = d.getConsensusId();
                        leadersIds[count] = d.getLeader();
                        regenciesIds[count] = d.getRegency();

                        CertifiedDecision cDec = new CertifiedDecision(this.controller.getStaticConf().getProcessId(),
                                d.getConsensusId(), d.getValue(), d.getDecisionEpoch().proof);
                        cDecs[count] = cDec;
                        // cons.firstMessageProposed contains the performance counters
                        if (requests[count][0].equals(d.firstMessageProposed)) {
                            long time = requests[count][0].timestamp;
                            long seed = requests[count][0].seed;
                            int numOfNonces = requests[count][0].numOfNonces;
                            requests[count][0] = d.firstMessageProposed;
                            requests[count][0].timestamp = time;
                            requests[count][0].seed = seed;
                            requests[count][0].numOfNonces = numOfNonces;
                        }

                        count++;
                    }
                    Decision lastDecision = decisions.get(decisions.size() - 1);
//                     loop over consensus ids and log them
//                    logger.debug("Decisions to deliver: {}", decisions.size());
//                    for (int i = 0; i < consensusIds.length; i++) {
//                        logger.debug("Consensus id: {}", consensusIds[i]);
//                    }
//                    logger.debug("Delivering messages from consensus {} to {}", consensusIds, consensusIds[consensusIds.length - 1]);
                    deliverMessages(consensusIds, regenciesIds, leadersIds, cDecs, requests);

                    // ******* EDUARDO BEGIN ***********//
                    if (controller.hasUpdates()) {
                        processReconfigMessages(lastDecision.getConsensusId());
                    }
                    if (lastReconfig > -2 && lastReconfig <= lastDecision.getConsensusId()) {

                        // set the consensus associated to the last decision as the last executed
                        logger.debug("Setting last executed consensus to " + lastDecision.getConsensusId());
                        // define that end of this execution
                        tomLayer.setLastExecAndRemoveInExec(lastDecision.getConsensusId());
                        // ******* EDUARDO END **************//

                        lastReconfig = -2;
                    }

                    // define the last stable consensus... the stable consensus can
                    // be removed from the leaderManager and the executionManager
                    // TODO: Is this part necessary? If it is, can we put it
                    // inside setLastExec
                    int cid = lastDecision.getConsensusId();
                    if (cid > 2) {
                        int stableConsensus = cid - 3;

                        tomLayer.execManager.removeConsensus(stableConsensus);
                    }
                }
            } catch (Exception e) {
                logger.error("Error while processing decision", e);
            }

            // THIS IS JOAO'S CODE, TO HANDLE STATE TRANSFER
            //deliverUnlock();
            //******************************************************************
            deliverLock.unlock();
        }
        logger.info("DeliveryThread stopped.");

    }

    private TOMMessage[] extractMessagesFromDecision(Decision dec) {
        TOMMessage[] requests = dec.getDeserializedValue();
        if (requests == null) {
            // there are no cached deserialized requests
            // this may happen if this batch proposal was not verified
            // TODO: this condition is possible?

            logger.debug("Interpreting and verifying batched requests.");

            // obtain an array of requests from the decisions obtained
            BatchReader batchReader = new BatchReader(dec.getValue(), controller.getStaticConf().getUseSignatures() == 1);
            requests = batchReader.deserialiseRequests(controller);
        } else {
            logger.debug("Using cached requests from the propose.");
        }

        return requests;
    }

    public void deliverUnordered(TOMMessage request, int regency) {

        MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(), request.getReqType(),
                request.getSession(), request.getSequence(), request.getOperationId(), request.getReplyServer(),
                request.serializedMessageSignature, System.currentTimeMillis(), 0, 0, regency, -1, -1, null, null,
                false); // Since the request is unordered,
        // there is no consensus info to pass

        msgCtx.readOnly = true;
        receiver.receiveReadonlyMessage(request, msgCtx);
    }

    private void deliverMessages(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs,
                                 TOMMessage[][] requests) {
        receiver.receiveMessages(consId, regencies, leaders, cDecs, requests);
    }

    private void processReconfigMessages(int consId) {
        byte[] response = controller.executeUpdates(consId);
        TOMMessage[] dests = controller.clearUpdates();

        if (controller.getCurrentView().isMember(receiver.getId())) {
            for (TOMMessage dest : dests) {
                tomLayer.getCommunication().send(new int[]{dest.getSender()},
                        new TOMMessage(controller.getStaticConf().getProcessId(), dest.getSession(),
                                dest.getSequence(), dest.getOperationId(), response,
                                controller.getCurrentViewId(), TOMMessageType.RECONFIG));
            }

            tomLayer.getCommunication().updateServersConnections();
        } else {
            receiver.restart();
        }
    }

    public void shutdown() {
        this.doWork = false;

        logger.info("Shutting down delivery thread");

        decidedLock.lock();
        notEmptyQueue.signalAll();
        decidedLock.unlock();
    }

    /*
     * public int size() { return decided.size(); }
     */
}