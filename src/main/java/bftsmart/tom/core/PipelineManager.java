package bftsmart.tom.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PipelineManager {
    private int maxAllowedConsensusesInExec;
    private int maxConsensusesInExec;
    private int waitForNextConsensusTime;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<Integer> consensusesInExecution = new ArrayList<>();
    private Long timestamp_LastConsensusStarted = 0L;
    private List<Integer> suggestedAmountOfConsInPipelineList = new ArrayList<>();
    private List<Long> latencyList = new ArrayList<>();

    public PipelineManager(int maxConsensusesInExec, int waitForNextConsensusTime) {
        this.maxConsensusesInExec = maxConsensusesInExec;
        this.waitForNextConsensusTime = waitForNextConsensusTime;
        this.maxAllowedConsensusesInExec = maxConsensusesInExec;
    }

    public long getAmountOfMillisecondsToWait() {
        return Math.max(waitForNextConsensusTime - (TimeUnit.MILLISECONDS.convert((System.nanoTime() - timestamp_LastConsensusStarted), TimeUnit.NANOSECONDS)), 0);
    }

    public int getMaxConsensusesInExec() {
        return maxConsensusesInExec;
    }

    public boolean isDelayedBeforeNewConsensusStart() {
        return consensusesInExecution.size() == 0 || getAmountOfMillisecondsToWait() <= 0;
    }

    public List<Integer> getConsensusesInExecution() {
        return this.consensusesInExecution;
    }

    public boolean isAllowedToAddToConsensusInExecList() {
        return this.consensusesInExecution.size() < maxConsensusesInExec;
    }

    public void addToConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.contains(cid) && isAllowedToAddToConsensusInExecList()) {
            this.consensusesInExecution.add(cid);
            timestamp_LastConsensusStarted = System.nanoTime();
            logger.debug("Adding to consensusesInExecution value " + (cid));
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.debug("Value {} already exist in consensusesInExecution list or the list is full. List size {}: ", cid, this.consensusesInExecution.size());
        }
    }

    public void removeFromConsensusInExecList(int cid) {
        if (!this.consensusesInExecution.isEmpty() && this.consensusesInExecution.contains(cid)) {
            this.consensusesInExecution.remove((Integer) cid);
            logger.debug("Removing in consensusesInExecution value: {}", cid);
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.warn("Cannot remove value {} in consensusesInExecution list because value not in the list.", cid);
        }
    }

    public void cleanUpConsensusesInExec() {
        this.consensusesInExecution = new ArrayList<>();
    }

    public void updatePipelineConfiguration(long latencyInNanoseconds, long messageSizeInBytes, int[] amountOfReplicas) {
        Random random = new Random();
        int bandwidth = random.nextInt(51) + 30;
        int bandwidthInBit = bandwidth * 1024 * 1024;
        logger.debug("Message size in bytes: {}", messageSizeInBytes);
        logger.debug("bandwidthInBit: {}bit/s", bandwidthInBit);
        logger.debug("latencyInNanoseconds: {}", latencyInNanoseconds);

        long latencyInMilliseconds = TimeUnit.MILLISECONDS.convert(latencyInNanoseconds, TimeUnit.NANOSECONDS);
        if (messageSizeInBytes <= 0L || bandwidth <= 0L || latencyInMilliseconds <= 0L) {
            logger.debug("Message size, bandwidth or latency is not set or extremely small. Skipping pipeline configuration update.");
            return;
        }

        Integer currentSuggestedAmountOfConsInPipeline = calculateCurrentSuggestedAmountOfConsInPipeline(messageSizeInBytes, amountOfReplicas, bandwidthInBit, (double) latencyInMilliseconds);
        if (currentSuggestedAmountOfConsInPipeline == 0) return; // TODO should we take actions here?

        if (this.suggestedAmountOfConsInPipelineList.size() >= 100) {
            calculateAndSetNewConfigsForPipeline();
        } else {
            this.suggestedAmountOfConsInPipelineList.add(currentSuggestedAmountOfConsInPipeline);
            this.latencyList.add(latencyInMilliseconds);
            logger.debug("Not enough measurements to update pipeline configuration, size: {}. Current measurement added: {} with latency: {}\n list: {}", this.suggestedAmountOfConsInPipelineList.size(), currentSuggestedAmountOfConsInPipeline, latencyInMilliseconds, this.suggestedAmountOfConsInPipelineList);
        }
    }

    private Integer calculateCurrentSuggestedAmountOfConsInPipeline(long messageSizeInBytes, int[] amountOfReplicas, int bandwidthInBit, double latencyInMilliseconds) {
        BigDecimal messageSize = new BigDecimal(messageSizeInBytes);
        BigDecimal bandwidthInBits = new BigDecimal(bandwidthInBit);
        // time needed for broadcast to one replica in seconds
        BigDecimal totalTransferTimeInMilliseconds = messageSize.multiply(new BigDecimal(8))
                .divide(bandwidthInBits, new MathContext(10))
                .multiply(new BigDecimal(1000))
                .multiply(new BigDecimal(amountOfReplicas.length));
        logger.debug("Total time for broadcasting to all replicas: {}ms", totalTransferTimeInMilliseconds);

        if (totalTransferTimeInMilliseconds.compareTo(BigDecimal.ZERO) == 0) {
            logger.debug("Total transfer time is 0. Skipping pipeline configuration update.");
            return 0;
        }
        int newMaxConsInExec = (int) Math.round(latencyInMilliseconds / (totalTransferTimeInMilliseconds.doubleValue() * 2));
        return newMaxConsInExec;
    }

    private void calculateAndSetNewConfigsForPipeline() {
        int averageSuggestedAmountOfConsInPipeline = (int) Math.round(this.suggestedAmountOfConsInPipelineList.stream().mapToInt(a -> a).average().getAsDouble());
        long averageLatency = (int) Math.round(this.latencyList.stream().mapToLong(a -> a).average().getAsDouble());
        logger.debug("Calculated averageSuggestedAmountOfConsInPipeline: {}", averageSuggestedAmountOfConsInPipeline);
        logger.debug("Calculated averageLatency: {}ms", averageLatency);

        if(averageSuggestedAmountOfConsInPipeline > maxAllowedConsensusesInExec){
            averageSuggestedAmountOfConsInPipeline = maxAllowedConsensusesInExec;
        }

        if (averageSuggestedAmountOfConsInPipeline != maxConsensusesInExec && averageSuggestedAmountOfConsInPipeline > 0 ) {
            maxConsensusesInExec = averageSuggestedAmountOfConsInPipeline;
            int newWaitForNextConsensusTime = (int) Math.round((double) averageLatency / (double) maxConsensusesInExec);
            waitForNextConsensusTime = newWaitForNextConsensusTime;

            logger.debug("=======Updating pipeline configuration=======");
            logger.debug("Current consensusesInExecution: {}", consensusesInExecution.toString());
            logger.debug("New maxConsensusesInExec: {}", maxConsensusesInExec);
            logger.debug("New waitForNextConsensusTime: {}ms", waitForNextConsensusTime);
        } else if (averageSuggestedAmountOfConsInPipeline == 0) { // should not be the cast at all.
            if(maxConsensusesInExec >= 10) {
                maxConsensusesInExec = 5;
            } else if(maxConsensusesInExec >= 5) {
                maxConsensusesInExec = 3;
            } else {
                maxConsensusesInExec = 1;
            }
            int newWaitForNextConsensusTime = (int) Math.round((double) averageLatency / (double) maxConsensusesInExec);
            waitForNextConsensusTime = newWaitForNextConsensusTime;
            logger.debug("=======Updating pipeline configuration=======");
            logger.debug("Current consensusesInExecution: {}", consensusesInExecution.toString());
            logger.debug("New maxConsensusesInExec: {}", maxConsensusesInExec);
            logger.debug("New waitForNextConsensusTime: {}ms", waitForNextConsensusTime);
        }
        this.suggestedAmountOfConsInPipelineList.clear();
    }
}
