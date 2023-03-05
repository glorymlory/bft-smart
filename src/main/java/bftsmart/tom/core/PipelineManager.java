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
    private final int maxAllowedConsensusesInExec;
    private final int maxWaitForNextConsensusTime;
    private final int reconfigurationTimerModeTime;
    private int currentMaxConsensusesInExec;
    private int waitForNextConsensusTime;

    private AtomicLong lastConsensusId = new AtomicLong();
    Set<Integer> consensusesInExecution = ConcurrentHashMap.<Integer>newKeySet();

    private Long timestamp_LastConsensusStarted = 0L;
    private BigDecimal bandwidthInBit;

    private List<Integer> suggestedAmountOfConsInPipelineList = new ArrayList<>();
    private List<Long> latencyList = new ArrayList<>();

    private boolean isProcessingReconfiguration = false;
    private boolean isReconfigurationTimerStarted = false;
    private List<Integer> reconfigurationReplicasToBeAdded = new ArrayList<>();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public PipelineManager(int maxConsensusesInExec, int waitForNextConsensusTime, int bandwidthMibit) {
        this.currentMaxConsensusesInExec = maxConsensusesInExec;
        this.maxAllowedConsensusesInExec = maxConsensusesInExec;
        this.waitForNextConsensusTime = waitForNextConsensusTime;
        this.maxWaitForNextConsensusTime = 80;
        reconfigurationTimerModeTime = 200; // 200 ms
        this.lastConsensusId.set(-1);
        this.bandwidthInBit = BigDecimal.valueOf(bandwidthMibit*1048576);
    }

    public long getAmountOfMillisecondsToWait() {
        return Math.max(waitForNextConsensusTime - (TimeUnit.MILLISECONDS.convert((System.nanoTime() - timestamp_LastConsensusStarted), TimeUnit.NANOSECONDS)), 0);
    }

    public int getCurrentMaxConsensusesInExec() {
        return currentMaxConsensusesInExec;
    }

    public boolean isDelayedBeforeNewConsensusStart() {
        return consensusesInExecution.size() == 0 || getAmountOfMillisecondsToWait() <= 0;
    }

    public Set<Integer> getConsensusesInExecution() {
        return this.consensusesInExecution;
    }

    public boolean isAllowedToAddToConsensusInExecList() {
        return this.consensusesInExecution.size() < currentMaxConsensusesInExec;
    }

    public void addToConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.contains(cid) && isAllowedToAddToConsensusInExecList()) {
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

    public void monitorPipelineLoad(long writeLatencyInNanoseconds, long messagesSizeInBytes, int currentBatchSize, int maxBatchSize,  int amountOfReplicas) {
        logger.debug("Message size in bytes: {}", messagesSizeInBytes);
        logger.debug("bandwidthInBit: {}bit/s", bandwidthInBit);
        logger.debug("latencyInNanoseconds: {}", writeLatencyInNanoseconds);

        long latencyInMilliseconds = TimeUnit.MILLISECONDS.convert(writeLatencyInNanoseconds, TimeUnit.NANOSECONDS);

        if (messagesSizeInBytes <= 0L || bandwidthInBit.compareTo(BigDecimal.ZERO) == 0 || latencyInMilliseconds <= 0L) {
            logger.debug("Message size, bandwidth or latency is not set or extremely small. Skipping pipeline configuration update.");
            return;
        }

        int currentSuggestedAmountOfConsInPipeline = calculateNewAmountOfConsInPipeline(messagesSizeInBytes, amountOfReplicas, latencyInMilliseconds);

        this.suggestedAmountOfConsInPipelineList.add(currentSuggestedAmountOfConsInPipeline);
        this.latencyList.add(latencyInMilliseconds);

//        if(isConfigUpdatePaused()) {
//            return;
//        }

        if ((this.suggestedAmountOfConsInPipelineList.size() >= 1 || currentSuggestedAmountOfConsInPipeline==0) && !isProcessingReconfiguration) {
            updatePipelineConfiguration(currentBatchSize, maxBatchSize);
        }
    }

    private boolean isConfigUpdatePaused() {
        logger.debug("this.consensusesInExecution.size() >= this.currentMaxConsensusesInExec: {}", this.consensusesInExecution.size() >= this.currentMaxConsensusesInExec);
        return this.currentMaxConsensusesInExec < this.maxAllowedConsensusesInExec && this.consensusesInExecution.size() >= this.currentMaxConsensusesInExec;
    }

    private Integer calculateNewAmountOfConsInPipeline(long messageSizeInBytes, int amountOfReplicas, long latencyInMilliseconds) {
        double transmissionTimeMs = calculateTransmissionTime(Math.toIntExact(messageSizeInBytes), bandwidthInBit.doubleValue(),amountOfReplicas);
        logger.debug("Total time with propose and write transmission: {}ms", transmissionTimeMs);

        int newMaxConsInExec =  (int) Math.round(latencyInMilliseconds/transmissionTimeMs);
        logger.debug("Calculated max cons in exec: {}", newMaxConsInExec);
        logger.debug("Current max cons in exec: {}", currentMaxConsensusesInExec);
        return newMaxConsInExec;
    }

    private static double calculateTransmissionTime(int packetSizeBytes, double dataRateBits, int amountOfReplicas) {
        double packetSizeBits = packetSizeBytes * 8; // Convert packet size from bytes to bits
        double transmissionTimeSeconds = packetSizeBits / dataRateBits; // Calculate transmission time in seconds
        transmissionTimeSeconds = transmissionTimeSeconds * 1000; // Convert transmission time from seconds to milliseconds
        transmissionTimeSeconds = transmissionTimeSeconds * amountOfReplicas; // Multiply by amount of replicas
        return transmissionTimeSeconds*2; // Multiply by 2 to account for both propose and write
    }

    private void updatePipelineConfiguration(int batchSize, int maxBatchSize) {
        int averageSuggestedAmountOfConsInPipeline = (int) Math.round(this.suggestedAmountOfConsInPipelineList.stream().mapToInt(a -> a).average().getAsDouble());
        long averageLatency = (int) Math.round(this.latencyList.stream().mapToDouble(a -> a).average().getAsDouble());

        logger.debug("Calculated averageSuggestedAmountOfConsInPipeline: {}", averageSuggestedAmountOfConsInPipeline);

        if (averageSuggestedAmountOfConsInPipeline > maxAllowedConsensusesInExec) {
            averageSuggestedAmountOfConsInPipeline = maxAllowedConsensusesInExec;
        }

        if (averageSuggestedAmountOfConsInPipeline != currentMaxConsensusesInExec && averageSuggestedAmountOfConsInPipeline > 1) {
            currentMaxConsensusesInExec = averageSuggestedAmountOfConsInPipeline;
            int newWaitForNextConsensusTime = (int) Math.round((double) averageLatency / (double) currentMaxConsensusesInExec);
            if(newWaitForNextConsensusTime > 0 && newWaitForNextConsensusTime < maxWaitForNextConsensusTime) {
                waitForNextConsensusTime = newWaitForNextConsensusTime;
            } else {
                waitForNextConsensusTime = maxWaitForNextConsensusTime;
            }
        }

        if(batchSize == maxBatchSize && currentMaxConsensusesInExec == 1) {
            currentMaxConsensusesInExec = 3;
            waitForNextConsensusTime = 0;
        } else if (averageSuggestedAmountOfConsInPipeline == 0 || averageSuggestedAmountOfConsInPipeline==1) { // should not be the cast at all.
            logger.debug("Average suggested amount of consensuses in pipeline is 0. Should not be the case.");
            currentMaxConsensusesInExec = 1;
            waitForNextConsensusTime = 0;
        }

        logger.debug("=======Updating pipeline configuration=======");
        logger.debug("Current consensusesInExecution: {}", consensusesInExecution.toString());
        logger.debug("New maxConsensusesInExec: {}", currentMaxConsensusesInExec);
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
        currentMaxConsensusesInExec = 1;

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