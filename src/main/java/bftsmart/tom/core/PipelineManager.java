package bftsmart.tom.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PipelineManager {
    // only for the leader
    private final int maxAllowedConsensusesInExecFixed;
    private final int maxWaitForNextConsensusTime;
    private final int reconfigurationTimerModeTime;

    private int maxConsToStartInParallel;
    private int waitForNextConsensusTime;
    private AtomicLong lastConsensusId = new AtomicLong();
    Set<Integer> consensusesInExecution = ConcurrentHashMap.<Integer>newKeySet();
    private Long timestamp_LastConsensusStarted = 0L;

    private BigDecimal bandwidthInBit;
    private int maxBatchSize = 0;
    private List<Integer> suggestedAmountOfConsInPipelineList = new ArrayList<>();
    private List<Long> latencyList = new ArrayList<>();
    private int lastConsensusLatency = 0;

    private boolean isProcessingReconfiguration = false;
    private boolean isReconfigurationTimerStarted = false;
    private List<Integer> reconfigurationReplicasToBeAdded = new ArrayList<>();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public PipelineManager(int maxConsensusesInExec, int waitForNextConsensusTime, int bandwidthMibit, int maxBatchSize) {
        this.maxAllowedConsensusesInExecFixed = maxConsensusesInExec;
        this.maxConsToStartInParallel = maxConsensusesInExec;
        this.waitForNextConsensusTime = waitForNextConsensusTime;
        this.maxBatchSize = maxBatchSize;
        this.bandwidthInBit = BigDecimal.valueOf(bandwidthMibit*1048576);

        this.maxWaitForNextConsensusTime = 40;
        this.reconfigurationTimerModeTime = 1200; // 200 ms
        this.lastConsensusId.set(-1);
    }

    public long getAmountOfMillisecondsToWait() {
        return Math.max(waitForNextConsensusTime - (TimeUnit.MILLISECONDS.convert((System.nanoTime() - timestamp_LastConsensusStarted), TimeUnit.NANOSECONDS)), 0);
    }

    public int getMaxAllowedConsensusesInExecFixed() {
        return maxAllowedConsensusesInExecFixed;
    }

    public boolean isDelayedBeforeNewConsensusStart() {
        return consensusesInExecution.size() == 0 || getAmountOfMillisecondsToWait() <= 0;
    }

    public Set<Integer> getConsensusesInExecution() {
        return this.consensusesInExecution;
    }

    public boolean isAllowedToProcessConsensus() {
        return this.consensusesInExecution.size() < maxAllowedConsensusesInExecFixed;
    }

    public boolean isAllowedToStartNewConsensus() {
        return this.consensusesInExecution.size() < maxConsToStartInParallel;
    }

    public void addToConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.contains(cid) && isAllowedToProcessConsensus()) {
            this.consensusesInExecution.add(cid);
            timestamp_LastConsensusStarted = System.nanoTime();
            logger.info("Adding to consensusesInExecution value " + (cid));
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());

//            keep track of the consensus with the highest id
            if (cid > this.lastConsensusId.get()) {
                this.lastConsensusId.set(cid);
            }
        } else {
            logger.debug("Value {} already exist in consensusesInExecution list or the list is full. List size {}: ", cid, this.consensusesInExecution.size());
        }
    }

    public void removeFromConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.isEmpty() && this.consensusesInExecution.contains(cid)) {
            this.consensusesInExecution.remove((Integer) cid);
            logger.info("Removing in consensusesInExecution value: {}", cid);
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.warn("Cannot remove value {} in consensusesInExecution list because value not in the list.", cid);
        }
    }

    public void cleanUpConsensusesInExec() {
        this.consensusesInExecution = ConcurrentHashMap.<Integer>newKeySet();
    }

    public void monitorPipelineLoad(long writeLatencyInNanoseconds, long messagesSizeInBytes,  int amountOfReplicas) {
        long latencyInMilliseconds = TimeUnit.MILLISECONDS.convert(writeLatencyInNanoseconds, TimeUnit.NANOSECONDS);
        lastConsensusLatency = (int) latencyInMilliseconds;
    }

    public void decideOnMaxAmountOfConsensuses(int countPendingRequests, int totalMessageSizeForMaxOrGivenBatch, int amountOfReplicas) {
        logger.debug("countPendingRequests: {}", countPendingRequests);
        logger.debug("bandwidthInBit: {} bit/s", bandwidthInBit);
        logger.debug("totalMessageSize: {} bytes", totalMessageSizeForMaxOrGivenBatch);
        logger.debug("avgLatency: {} ms", lastConsensusLatency);

        if (lastConsensusLatency <= 0L || bandwidthInBit.compareTo(BigDecimal.ZERO) == 0 || countPendingRequests <= 0L || totalMessageSizeForMaxOrGivenBatch <= 0L) {
            logger.debug("Not enough information to decide on max amount of consensuses. Returning.");
            return;
        }

        int currentSuggestedAmountOfConsInPipeline = calculateNewAmountOfConsInPipeline(totalMessageSizeForMaxOrGivenBatch, amountOfReplicas, lastConsensusLatency);
        this.suggestedAmountOfConsInPipelineList.add(currentSuggestedAmountOfConsInPipeline);

        int highLoadSuggestedAmountOfConsInPipeline = 0;
        int suggestedDelay = 0;

        if(countPendingRequests > (2*maxBatchSize)) {
            highLoadSuggestedAmountOfConsInPipeline = countPendingRequests / maxBatchSize;
            if(highLoadSuggestedAmountOfConsInPipeline <= maxConsToStartInParallel){
                highLoadSuggestedAmountOfConsInPipeline = maxConsToStartInParallel + 1;
                suggestedDelay = 5;
            }
            if(highLoadSuggestedAmountOfConsInPipeline > 10){
                highLoadSuggestedAmountOfConsInPipeline = maxConsToStartInParallel;
                suggestedDelay = 20;
            }
            logger.debug("HIGH LOAD: Current suggested amount of cons in pipeline: {}", highLoadSuggestedAmountOfConsInPipeline);
        }

        if (!isProcessingReconfiguration) {
            updatePipelineConfiguration(Math.max(highLoadSuggestedAmountOfConsInPipeline, currentSuggestedAmountOfConsInPipeline), lastConsensusLatency, suggestedDelay);
        }
    }

    private Integer calculateNewAmountOfConsInPipeline(long messageSizeInBytes, int amountOfReplicas, long latencyInMilliseconds) {
        double transmissionTimeMs = calculateTransmissionTime(Math.toIntExact(messageSizeInBytes), bandwidthInBit.doubleValue(), amountOfReplicas);
        logger.debug("Total time with propose and write transmission: {} ms", transmissionTimeMs);

        int newMaxConsInExec =  (int) Math.round(latencyInMilliseconds/transmissionTimeMs);

        logger.debug("Calculated max cons in exec: {}", newMaxConsInExec);
        logger.debug("Current max cons in exec: {}", maxConsToStartInParallel);
        return newMaxConsInExec;
    }

    private static double calculateTransmissionTime(int packetSizeBytes, double dataRateBits, int amountOfReplicas) {
        double packetSizeBits = packetSizeBytes * 8; // Convert packet size from bytes to bits
        double transmissionTimeSeconds = packetSizeBits / dataRateBits; // Calculate transmission time in seconds
        transmissionTimeSeconds = transmissionTimeSeconds * 1000; // Convert transmission time from seconds to milliseconds
        transmissionTimeSeconds = transmissionTimeSeconds * amountOfReplicas; // Multiply by amount of replicas
        return transmissionTimeSeconds; // Multiply by 2 to account for both propose and write
    }

    private void updatePipelineConfiguration(int newMaxConsInExec, int latency, int suggestedDelay) {
        if (newMaxConsInExec > maxAllowedConsensusesInExecFixed) {
            newMaxConsInExec = maxAllowedConsensusesInExecFixed;
        }

        if (newMaxConsInExec != maxConsToStartInParallel && newMaxConsInExec >= 1) {
            maxConsToStartInParallel = newMaxConsInExec;
            int newWaitForNextConsensusTime = (int) Math.round((double) latency / (double) maxConsToStartInParallel);
            if(newWaitForNextConsensusTime < maxWaitForNextConsensusTime) {
                waitForNextConsensusTime = newWaitForNextConsensusTime;
            } else {
                waitForNextConsensusTime = maxWaitForNextConsensusTime;
            }

            if(suggestedDelay>0){
                waitForNextConsensusTime = suggestedDelay;
            }
        } else if (newMaxConsInExec == 0) { // should not be the cast at all.
            logger.debug("Average suggested amount of consensuses in pipeline is 0. Should not be the case.");
            maxConsToStartInParallel = 3;
            waitForNextConsensusTime = 20;
        }

        logger.debug("=======Updating pipeline configuration=======");
        logger.debug("Current consensusesInExecution: {}", consensusesInExecution.toString());
        logger.debug("New maxConsensusesInExec: {}", maxConsToStartInParallel);
        logger.debug("New waitForNextConsensusTime: {}ms", waitForNextConsensusTime);
        this.suggestedAmountOfConsInPipelineList.clear();
        this.latencyList.clear();
    }

    public long getNewConsensusIdAndIncrement() {
        return this.lastConsensusId.incrementAndGet();
    }

    public long getLastConsensusId() {
        return this.lastConsensusId.get();
    }

    public void setPipelineInReconfigurationMode() {
        logger.debug("Waiting for new replica join: {}");
        isProcessingReconfiguration = true;
        maxConsToStartInParallel = 1;

        validateReconfigurationModeStatus();
    }

    public void validateReconfigurationModeStatus() {
        if (isProcessingReconfiguration && !isReconfigurationTimerStarted && consensusesInExecution.size() <= 1) {
            isReconfigurationTimerStarted = true;
            setReconfigurationTimer();
        }
    }

    public void setPipelineOutOfReconfigurationMode() {
        logger.debug("Reconfiguration mode for pipeline finished. New replicas: {}", reconfigurationReplicasToBeAdded.toString());
        isProcessingReconfiguration = false;
        isReconfigurationTimerStarted = false;
        reconfigurationReplicasToBeAdded.clear();
    }

    public void setReconfigurationTimer() {
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (consensusesInExecution.size() <= 1) {
                    setPipelineOutOfReconfigurationMode();
                }
            }
        };

        timer.schedule(task, reconfigurationTimerModeTime);
    }
}