package bftsmart.tom.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PipelineManager {
    private int maxConsensusesInExec;
    private int waitForNextConsensusTime;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<Integer> consensusesInExecution = new ArrayList<>();
    private Long timestamp_LastConsensusStarted = 0L;
    private long batchDisseminationTimeInMilliseconds = 0L;

    public PipelineManager(int maxConsensusesInExec, int waitForNextConsensusTime) {
        this.maxConsensusesInExec = maxConsensusesInExec;
        this.waitForNextConsensusTime = waitForNextConsensusTime;
    }

    public long getAmountOfMillisecondsToWait() {
        return Math.max(waitForNextConsensusTime - (TimeUnit.MILLISECONDS.convert((System.nanoTime() - timestamp_LastConsensusStarted), TimeUnit.NANOSECONDS)), 0);
    }

    public int getMaxConsensusesInExec() {
        logger.debug("Max consensuses in execution: {} ", maxConsensusesInExec);
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

    public void updatePipelineConfiguration(long latencyInNanoseconds, long messageSizeInBytes) {


        long latencyInMilliseconds = TimeUnit.MILLISECONDS.convert(latencyInNanoseconds, TimeUnit.NANOSECONDS);
        if (latencyInMilliseconds <= 0L) {
            logger.debug("Consensus latency is not set or extremely small. Skipping pipeline configuration update.");
            return;
        }
        int bandwidth = 100;
        int bandwidthInBit = bandwidth * 1024 * 1024;

        long timeNeeded = (long) (messageSizeInBytes * 8 / (bandwidth * 1000000)); // Convert Mibit to bits and divide by number of bits to get seconds
        logger.debug("Time needed for broadcast: {}s", timeNeeded);

        long timeNeededForBroadcast = (long) (messageSizeInBytes * 8 / (bandwidthInBit));
        long transferTimeInMilliseconds = timeNeededForBroadcast * 1000;

        logger.debug("Time needed for broadcast: {}ms", transferTimeInMilliseconds);

        if (transferTimeInMilliseconds <= 0L) {
            logger.debug("Transfer time is not set or extremely small. Skipping pipeline configuration update.");
//            return;
        }


        int newMaxConsInExec = (int) Math.round((double) latencyInMilliseconds / (double) (transferTimeInMilliseconds*2));
//        why by 2, because before starting a new consensus we have to finish propose dissemination and then "write sent" stage, because during "write sent"
//        we actually waiting for a reply and during propose we dont.


        if (newMaxConsInExec != maxConsensusesInExec && newMaxConsInExec > 0) {
//            maxConsensusesInExec = newMaxConsInExec;
            logger.debug("New maxConsensusesInExec: {}", newMaxConsInExec);
            int newWaitForNextConsensusTime = (int) Math.round((double) latencyInMilliseconds / (double) maxConsensusesInExec);
//            waitForNextConsensusTime = newWaitForNextConsensusTime;
            logger.debug("New waitForNextConsensusTime: {}ms", newWaitForNextConsensusTime);
        } else if(newMaxConsInExec <= 0) {
//            maxConsensusesInExec = 1; // we set it to 1 due to leader not being able to execute more than one consensus at a time
            logger.debug("New maxConsensusesInExec: {}. Setting as default due to the leader load problems.", maxConsensusesInExec);
            return;
        }

        logger.debug("Updating pipeline configuration");
        logger.debug("Current consensusesInExecution: {}", consensusesInExecution.toString());
        logger.debug("Current maxConsensusesInExec: {}", maxConsensusesInExec);
        logger.debug("Current waitForNextConsensusTime: {}ms", waitForNextConsensusTime);
        logger.debug("Current batchDisseminationTime: {}ms", batchDisseminationTimeInMilliseconds);
        logger.debug("Current consensusLatency: {}ms", latencyInMilliseconds);
    }


    public void setBatchDisseminationTimeInMilliseconds(long batchDisseminationTimeInNanoSeconds) {
        this.batchDisseminationTimeInMilliseconds = TimeUnit.MILLISECONDS.convert(batchDisseminationTimeInNanoSeconds, TimeUnit.NANOSECONDS);
    }
}
