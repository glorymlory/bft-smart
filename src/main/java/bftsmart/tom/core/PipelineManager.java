package bftsmart.tom.core;

import bftsmart.statemanagement.SMMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class PipelineManager {
    // Maximum allowed consensuses in execution (fixed value)
    private final int maxAllowedConsensusesInExecFixed;
    // Maximum wait time for next consensus
    private final int maxWaitForNextConsensusTime;

    // Maximum consensuses allowed to start in parallel
    private int maxConsToStartInParallel;
    // Wait time for the next consensus
    private int waitForNextConsensusTime;
    // The last consensus ID
    private AtomicLong lastConsensusId = new AtomicLong();
    // Set of consensuses currently in execution
    Set<Integer> consensusesInExecution = ConcurrentHashMap.<Integer>newKeySet();
    // Timestamp of when the last consensus started
    private Long timestamp_LastConsensusStarted = 0L;

    // Bandwidth in bits
    private BigDecimal bandwidthInBit;
    // Maximum batch size
    private int maxBatchSize;

    // The last consensus latency
    private long lastConsensusLatency = 0;

    // Indicates if the pipeline is in reconfiguration mode
    private boolean isReconfigurationMode = false;
    // Queue for reconfiguration SM messages
    private BlockingQueue<SMMessage> reconfigurationSMMessagesQueue;
    // Queue for reconfiguration consensus IDs
    private BlockingQueue<Integer> reconfigurationCIDsQueue;

    private final Object performanceDataLock = new Object();


    // Logger instance
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor for PipelineManager.
     *
     * @param maxConsensusesInExec     The maximum allowed consensuses in execution.
     * @param waitForNextConsensusTime The wait time for the next consensus.
     * @param bandwidthMibit           The bandwidth in Mibit.
     * @param maxBatchSize             The maximum batch size.
     */
    public PipelineManager(int maxConsensusesInExec, int waitForNextConsensusTime, int bandwidthMibit, int maxBatchSize) {
        this.maxAllowedConsensusesInExecFixed = maxConsensusesInExec;
        this.maxConsToStartInParallel = maxConsensusesInExec;
        this.waitForNextConsensusTime = waitForNextConsensusTime;
        this.maxBatchSize = maxBatchSize;
        this.bandwidthInBit = BigDecimal.valueOf(bandwidthMibit * 1048576);

        reconfigurationSMMessagesQueue = new LinkedBlockingQueue<>();
        reconfigurationCIDsQueue = new LinkedBlockingQueue<>();

        this.maxWaitForNextConsensusTime = 40;
        this.lastConsensusId.set(-1);
    }

    /**
     * Get the amount of milliseconds to wait before starting the next consensus.
     *
     * @return The amount of milliseconds to wait.
     */
    public long getAmountOfMillisecondsToWait() {
        return Math.max(waitForNextConsensusTime - (TimeUnit.MILLISECONDS.convert((System.nanoTime() - timestamp_LastConsensusStarted), TimeUnit.NANOSECONDS)), 0);
    }

    /**
     * Get the maximum allowed consensuses in execution (fixed value).
     *
     * @return The maximum allowed consensuses in execution (fixed value).
     */
    public int getMaxAllowedConsensusesInExecFixed() {
        return maxAllowedConsensusesInExecFixed;
    }

    /**
     * Check if the pipeline is delayed before starting a new consensus.
     *
     * @return true if the pipeline is delayed, false otherwise
     */
    public boolean isDelayedBeforeNewConsensusStart() {
        return consensusesInExecution.size() == 0 || getAmountOfMillisecondsToWait() <= 0;
    }

    /**
     * Get the current set of consensus instances in execution.
     *
     * @return a set containing the consensus instances in execution
     */
    public Set<Integer> getConsensusesInExecution() {
        return this.consensusesInExecution;
    }

    /**
     * Check if the pipeline is allowed to process a consensus.
     *
     * @return true if the pipeline is allowed to process a consensus, false otherwise
     */
    public boolean isAllowedToProcessConsensus() {
        return this.consensusesInExecution.size() < maxAllowedConsensusesInExecFixed;
    }

    /**
     * Check if the pipeline is allowed to start a new consensus.
     *
     * @return true if the pipeline is allowed to start a new consensus, false otherwise
     */
    public boolean isAllowedToStartNewConsensus() {
        return this.consensusesInExecution.size() < maxConsToStartInParallel;
    }

    /**
     * Add the given consensus ID to the list of consensuses in execution.
     *
     * @param cid the consensus ID to be added
     */
    public void addToConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.contains(cid) && isAllowedToProcessConsensus()) {
            this.consensusesInExecution.add(cid);
            timestamp_LastConsensusStarted = System.nanoTime();
            logger.info("Adding to consensusesInExecution value " + (cid));
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());

            // Keep track of the consensus with the highest id
            if (cid > this.lastConsensusId.get()) {
                this.lastConsensusId.set(cid);
            }
        } else {
            logger.debug("Value {} already exists in consensusesInExecution list or the list is full. List size {}: ", cid, this.consensusesInExecution.size());
        }
    }

    /**
     * Remove the given consensus ID from the list of consensuses in execution.
     *
     * @param cid the consensus ID to be removed
     */
    public void removeFromConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.isEmpty() && this.consensusesInExecution.contains(cid)) {
            this.consensusesInExecution.remove((Integer) cid);
            logger.info("Removing in consensusesInExecution value: {}", cid);
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.warn("Cannot remove value {} in consensusesInExecution list because value not in the list.", cid);
        }
    }

    /**
     * Clear the list of consensuses in execution.
     */
    public void cleanUpConsensusesInExec() {
        this.consensusesInExecution = ConcurrentHashMap.<Integer>newKeySet();
    }

    /**
     * Collect performance data for a consensus instance.
     *
     * @param cid                       the consensus ID
     * @param latencyInNanoseconds write latency in nanoseconds
     */
    public void collectConsensusPerformanceData(long cid, long latencyInNanoseconds) {
        long latencyInMilliseconds = TimeUnit.NANOSECONDS.toMillis(latencyInNanoseconds);

        synchronized (performanceDataLock) {
            lastConsensusLatency = latencyInMilliseconds;
        }
    }

    /**
     * Decide on the max amount of consensuses in the pipeline.
     *
     * @param countPendingRequests the number of pending requests
     * @param totalMessageSizeForMaxOrGivenBatch total size of messages for max or given batch
     * @param amountOfReplicas the number of replicas
     */
    public void decideOnMaxAmountOfConsensuses(int countPendingRequests, int totalMessageSizeForMaxOrGivenBatch, int amountOfReplicas) {
        logger.debug("last cons started : {}", lastConsensusId.get());
        logger.debug("countPendingRequests: {}", countPendingRequests);
        logger.debug("bandwidthInBit: {} bit/s", bandwidthInBit);
        logger.debug("totalMessageSize: {} bytes", totalMessageSizeForMaxOrGivenBatch);
        logger.debug("avgLatency: {} ms", lastConsensusLatency);

        if (lastConsensusLatency <= 0L || bandwidthInBit.compareTo(BigDecimal.ZERO) == 0 || totalMessageSizeForMaxOrGivenBatch <= 0L) {
            logger.debug("Not enough information to decide on max amount of consensuses. Returning.");
            return;
        }

        int currentSuggestedAmountOfConsInPipeline = calculateNewAmountOfConsInPipeline(totalMessageSizeForMaxOrGivenBatch, amountOfReplicas, lastConsensusLatency);

        if (!isReconfigurationMode) {
            updateMaxConsInExecAndConsensusTime(currentSuggestedAmountOfConsInPipeline, lastConsensusLatency);
        }
    }

    /**
     * Calculate the new amount of consensuses in the pipeline.
     *
     * @param messageSizeInBytes the message size in bytes
     * @param amountOfReplicas the number of replicas
     * @param latencyInMilliseconds the latency in milliseconds
     * @return the new amount of consensuses in the pipeline
     */
    private Integer calculateNewAmountOfConsInPipeline(long messageSizeInBytes, int amountOfReplicas, long latencyInMilliseconds) {
        double transmissionTimeProposeMs = calculateTransmissionTime(Math.toIntExact(messageSizeInBytes), bandwidthInBit.doubleValue(), amountOfReplicas);
        logger.debug("Total time for propose transmission: {} ms", transmissionTimeProposeMs);

        int newMaxConsInExec = (int) Math.round(latencyInMilliseconds / transmissionTimeProposeMs);
        logger.debug("Calculated max cons in exec: {}", newMaxConsInExec);
        logger.debug("Current max cons in exec: {}", maxConsToStartInParallel);

        return newMaxConsInExec;
    }

    /**
     * Calculate the transmission time.
     *
     * @param packetSizeBytes the packet size in bytes
     * @param dataRateBits the data rate in bits
     * @param amountOfReplicas the number of replicas
     * @return the transmission time
     */
    private static double calculateTransmissionTime(int packetSizeBytes, double dataRateBits, int amountOfReplicas) {
        double packetSizeBits = packetSizeBytes * 8; // Convert packet size from bytes to bits
        double transmissionTimeSeconds = packetSizeBits / dataRateBits; // Calculate transmission time in seconds
        transmissionTimeSeconds = transmissionTimeSeconds * 1000; // Convert transmission time from seconds to milliseconds
        transmissionTimeSeconds = transmissionTimeSeconds * amountOfReplicas; // Multiply by amount of replicas
        return transmissionTimeSeconds; // Multiply by 2 to account for both propose and write
    }

    /**
     * Update the pipeline configuration.
     *
     * @param newMaxConsInExec the new max consensuses in execution
     * @param latency the latency
     */
    private void updateMaxConsInExecAndConsensusTime(int newMaxConsInExec, long latency) {
        newMaxConsInExec = Math.min(newMaxConsInExec, maxAllowedConsensusesInExecFixed);

        if (newMaxConsInExec != maxConsToStartInParallel && newMaxConsInExec > 1) {
            maxConsToStartInParallel = newMaxConsInExec;
            waitForNextConsensusTime = getNewWaitForNextConsensusTime(latency);
        } else if (newMaxConsInExec <= 1) {
            logger.debug("Average suggested amount of consensuses in pipeline is less or equal to 1.");
            maxConsToStartInParallel = 1;
            waitForNextConsensusTime = 0;
        }

        logger.debug("=======Updating pipeline configuration=======");
        logger.debug("Current consensusesInExecution list: {}", consensusesInExecution.toString());
        logger.debug("New maxConsensusesInExec: {}", maxConsToStartInParallel);
        logger.debug("New waitForNextConsensusTime: {}ms", waitForNextConsensusTime);
    }


    /**
     * Get the new wait time for the next consensus.
     *
     * @param latency the latency
     * @return the new wait time for the next consensus
     */
    private int getNewWaitForNextConsensusTime(double latency) {
        int newWaitForNextConsensusTime = (int) Math.round(latency / (double) maxConsToStartInParallel);
        newWaitForNextConsensusTime = Math.min(newWaitForNextConsensusTime, maxWaitForNextConsensusTime);

        return newWaitForNextConsensusTime;
    }

    /**
     * Get a new consensus ID and increment the current value.
     *
     * @return the new consensus ID
     */
    public long getNewConsensusIdAndIncrement() {
        return this.lastConsensusId.incrementAndGet();
    }

    /**
     * Get the last consensus ID.
     *
     * @return the last consensus ID
     */
    public long getLastConsensusId() {
        return this.lastConsensusId.get();
    }

    /**
     * Set the highest initiated consensus ID.
     *
     * @param lastConsensusId the last consensus ID
     */
    public void setHighestInitiatedCID(long lastConsensusId) {
        this.lastConsensusId.set(lastConsensusId);
    }

    /**
     * Set the pipeline in reconfiguration mode.
     *
     * @param cid the consensus ID
     */
    public void setPipelineInReconfigurationMode(int cid) {
        logger.debug("Reconfiguration mode for pipeline started");
        isReconfigurationMode = true;
        maxConsToStartInParallel = 1;
        waitForNextConsensusTime = 0;
        reconfigurationCIDsQueue.add(cid);
    }

    /**
     * Check if reconfiguration is allowed to run for the given consensus ID.
     *
     * @param currentConsID the current consensus ID
     * @return true if reconfiguration is allowed, false otherwise
     */
    public boolean isAllowedToRunReconfiguration(int currentConsID) {
        SMMessage smMessage = reconfigurationSMMessagesQueue.peek();
        Integer lastReconfigCid = reconfigurationCIDsQueue.peek();
        if (isReconfigurationMode) {
            if (lastReconfigCid!=null && smMessage != null
                    && (lastReconfigCid + maxAllowedConsensusesInExecFixed) == currentConsID) {
                reconfigurationCIDsQueue.poll();
                return true;
            }
        }
        return false;
    }

    /**
     * Get the SMMessage to be used for reconfiguration.
     *
     * @return the SMMessage for reconfiguration
     */
    public SMMessage getSMMessageToReconfigure() {
        return reconfigurationSMMessagesQueue.poll();
    }

    /**
     * Set the pipeline out of reconfiguration mode.
     */
    public void setPipelineOutOfReconfigurationMode() {
        logger.debug("Reconfiguration mode for pipeline finished");
        isReconfigurationMode = false;
    }

    /**
     * Check if the pipeline is in reconfiguration mode.
     *
     * @return true if the pipeline is in reconfiguration mode, false otherwise
     */
    public boolean isReconfigurationMode() {
        return isReconfigurationMode;
    }

    /**
     * Schedule a replica reconfiguration with the given SMMessage.
     *
     * @param smsg the SMMessage to be used for reconfiguration
     */
    public void saveMessageForLaterReconfiguration(SMMessage smsg) {
        logger.debug("adding SMMessage : {}", smsg.getCID());
        reconfigurationSMMessagesQueue.add(smsg);
    }

}
