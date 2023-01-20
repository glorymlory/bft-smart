package bftsmart.tom.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PipelineManager {
    public final int maxConsensusesInExec;
    private final int waitForNextConsensusTime;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<Integer> consensusesInExecution = new ArrayList<>();
    private Long timestamp_LastConsensusStarted = 0L;

    public PipelineManager(int  maxConsensusesInExec , int waitForNextConsensusTime) {
        this.maxConsensusesInExec = maxConsensusesInExec;
        this.waitForNextConsensusTime = waitForNextConsensusTime;
    }

    public long getAmountOfMillisecondsToWait() {
        return Math.max(waitForNextConsensusTime - (TimeUnit.MILLISECONDS.convert((System.nanoTime() - timestamp_LastConsensusStarted),TimeUnit.NANOSECONDS)), 0);
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
            this.consensusesInExecution.remove((Integer)cid);
            logger.debug("Removing in consensusesInExecution value: {}", cid);
            logger.debug("Current consensusesInExecution : {} ", this.consensusesInExecution.toString());
        } else {
            logger.warn("Cannot remove value {} in consensusesInExecution list because value not in the list.", cid);
        }
    }

    public void cleanUpConsensusesInExec() {
        this.consensusesInExecution = new ArrayList<>();
    }
}
