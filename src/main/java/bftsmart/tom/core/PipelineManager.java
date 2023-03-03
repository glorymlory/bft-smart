package bftsmart.tom.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PipelineManager {
    private final int maxAllowedConsensusesInExec;
    private final int reconfigurationTimerModeTime;
    private int currentMaxConsensusesInExec;
    private int waitForNextConsensusTime;

    private AtomicLong lastConsensusId = new AtomicLong();
    Set<Integer> consensusesInExecution = ConcurrentHashMap.<Integer>newKeySet();

//    SpeedTestSocket speedTestSocket;
//    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
//    private boolean isBandwidthRunningRepeatedly = false;

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
        reconfigurationTimerModeTime = 200; // 200 ms
        this.lastConsensusId.set(-1);
        this.bandwidthInBit = BigDecimal.valueOf(bandwidthMibit*1048576);
//        this.speedTestSocket = new SpeedTestSocket();
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

    public void monitorPipelineLoad(long writeLatencyInNanoseconds, long messageSizeInBytes, int amountOfReplicas) {
//        if(!isBandwidthRunningRepeatedly) {
//            isBandwidthRunningRepeatedly = true;
//            startBandwidthMonitorRepeatedly();
//        }

//        BigDecimal bandwidthInBit = speedTestSocket.getLiveReport().getTransferRateBit();
        logger.debug("Message size in bytes: {}", messageSizeInBytes);
        logger.debug("bandwidthInBit: {}bit/s", bandwidthInBit);
        logger.debug("latencyInNanoseconds: {}", writeLatencyInNanoseconds);

        long latencyInMilliseconds = TimeUnit.MILLISECONDS.convert(writeLatencyInNanoseconds, TimeUnit.NANOSECONDS);

        if (messageSizeInBytes <= 0L || bandwidthInBit.compareTo(BigDecimal.ZERO) == 0 || latencyInMilliseconds <= 0L) {
            logger.debug("Message size, bandwidth or latency is not set or extremely small. Skipping pipeline configuration update.");
            return;
        }

        int currentSuggestedAmountOfConsInPipeline = calculateNewAmountOfConsInPipeline(messageSizeInBytes, amountOfReplicas, bandwidthInBit, latencyInMilliseconds);

        this.suggestedAmountOfConsInPipelineList.add(currentSuggestedAmountOfConsInPipeline);
        this.latencyList.add(latencyInMilliseconds);

        if ((this.suggestedAmountOfConsInPipelineList.size() >= 10 || currentSuggestedAmountOfConsInPipeline==0) && !isProcessingReconfiguration) {
            updatePipelineConfiguration();
        }
    }

    private void startBandwidthMonitorRepeatedly() {
        final int initialDelay = 0;
        final int period = 5; // seconds

//        addBandwidthListener();
//        scheduler.scheduleAtFixedRate(() -> {
//            try {
//                logger.debug("starting speed test");
//                speedTestSocket.startFixedDownload("https://speed.hetzner.de/100MB.bin", 1000, 500);
//            } catch (Exception e) {
//                logger.error("Error while getting bandwidth", e);
//            }
//        }, initialDelay, period, TimeUnit.SECONDS);
    }

    public void stopGettingBandwidthRepeatedlyAndRemoveListeners() {
//        isBandwidthRunningRepeatedly = false;
//        scheduler.shutdown();
//        try {
//            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
//                scheduler.shutdownNow();
//            }
//        } catch (InterruptedException e) {
//            scheduler.shutdownNow();
//            Thread.currentThread().interrupt();
//        }
//        speedTestSocket.closeSocket();
//        speedTestSocket.clearListeners();
    }

    private void addBandwidthListener() {
//        speedTestSocket.addSpeedTestListener(new ISpeedTestListener() {
//            @Override
//            public void onCompletion(SpeedTestReport report) {
//                // called when download/upload is complete
//                System.out.println("[COMPLETED] rate in bit/s   : " + report.getTransferRateBit());
//            }
//
//            @Override
//            public void onError(SpeedTestError speedTestError, String errorMessage) {
//                // called when a download/upload error occur
//            }
//
//            @Override
//            public void onProgress(float percent, SpeedTestReport report) {
//                // called to notify download/upload progress
//                System.out.println("[PROGRESS] progress : " + percent + "%");
//                System.out.println("[PROGRESS] rate in bit/s   : " + report.getTransferRateBit());
//            }
//        });
    }

    private Integer calculateNewAmountOfConsInPipeline(long messageSizeInBytes, int amountOfReplicas, BigDecimal bandwidthInBits, long latencyInMilliseconds) {
        BigDecimal messageSize = new BigDecimal(messageSizeInBytes);

        BigDecimal totalTransmissionTimeInMilliseconds = messageSize.multiply(new BigDecimal(8))
                .divide(bandwidthInBits, new MathContext(10))
                .multiply(new BigDecimal(1000))
                .multiply(new BigDecimal(amountOfReplicas));

        totalTransmissionTimeInMilliseconds = totalTransmissionTimeInMilliseconds.multiply(BigDecimal.valueOf(2));
        logger.debug("Total time with propose and write transmission: {}ms", totalTransmissionTimeInMilliseconds);

        int newMaxConsInExec = BigDecimal.valueOf(latencyInMilliseconds).divide(totalTransmissionTimeInMilliseconds,2, RoundingMode.HALF_UP).intValue();
        logger.debug("calculated max cons in exec: {}", newMaxConsInExec);
        logger.debug("current max cons in exec: {}", currentMaxConsensusesInExec);
        return newMaxConsInExec;
    }

    private void updatePipelineConfiguration() {
        int averageSuggestedAmountOfConsInPipeline = (int) Math.round(this.suggestedAmountOfConsInPipelineList.stream().mapToInt(a -> a).average().getAsDouble());
        long averageLatency = (int) Math.round(this.latencyList.stream().mapToDouble(a -> a).average().getAsDouble());

        logger.debug("Calculated averageSuggestedAmountOfConsInPipeline: {}", averageSuggestedAmountOfConsInPipeline);
        logger.debug("Calculated averageLatency: {}ms", averageLatency);

        if (averageSuggestedAmountOfConsInPipeline > maxAllowedConsensusesInExec) {
            averageSuggestedAmountOfConsInPipeline = maxAllowedConsensusesInExec;
        }

        if (averageSuggestedAmountOfConsInPipeline != currentMaxConsensusesInExec && averageSuggestedAmountOfConsInPipeline > 0) {
            currentMaxConsensusesInExec = averageSuggestedAmountOfConsInPipeline;
            int newWaitForNextConsensusTime = (int) Math.round((double) averageLatency / (double) currentMaxConsensusesInExec);
            waitForNextConsensusTime = newWaitForNextConsensusTime;
        }

        if (averageSuggestedAmountOfConsInPipeline == 0) { // should not be the cast at all.
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

    public long getNewConsensusId() {
        return this.lastConsensusId.incrementAndGet();
    }

    public void setPipelineInReconfigurationMode() {
        logger.debug("Waiting for new replica join: {}");
//        isProcessingReconfiguration = true;
//        currentMaxConsensusesInExec = 1;
//        reconfigurationReplicasToBeAdded.add(newReplicaId); // this value can be used to check if the new replica is already executing consensuses

        validateReconfigurationModeStatus();
    }

    public void validateReconfigurationModeStatus() {
        if (isProcessingReconfiguration && !isReconfigurationTimerStarted && consensusesInExecution.size() <= 1) {
//            isReconfigurationTimerStarted = true;
//            setReconfigurationTimer();
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