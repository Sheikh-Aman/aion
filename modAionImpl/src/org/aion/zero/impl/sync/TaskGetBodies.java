package org.aion.zero.impl.sync;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.aion.mcf.blockchain.BlockHeader;
import org.aion.p2p.IP2pMgr;
import org.aion.zero.impl.sync.msg.ReqBlocksBodies;
import org.aion.zero.impl.sync.statistics.RequestType;
import org.slf4j.Logger;

/**
 * long run
 *
 * @author chris
 */
final class TaskGetBodies implements Runnable {

    private final IP2pMgr p2p;

    private final AtomicBoolean run;

    private final BlockingQueue<HeadersWrapper> downloadedHeaders;

    private final SyncRequestManager syncRequestManager;

    private final Logger log, surveyLog;

    private final SyncStats stats;

    /**
     * @param _p2p IP2pMgr
     * @param _run AtomicBoolean
     * @param _downloadedHeaders BlockingQueue
     */
    TaskGetBodies(
            final IP2pMgr _p2p,
            final AtomicBoolean _run,
            final BlockingQueue<HeadersWrapper> _downloadedHeaders,
            final SyncRequestManager syncRequestManager,
            final SyncStats _stats,
            final Logger log,
            final Logger surveyLog) {
        this.p2p = _p2p;
        this.run = _run;
        this.downloadedHeaders = _downloadedHeaders;
        this.syncRequestManager = syncRequestManager;
        this.stats = _stats;
        this.log = log;
        this.surveyLog = surveyLog;
    }

    @Override
    public void run() {
        // for runtime survey information
        long startTime, duration;

        while (run.get()) {
            startTime = System.nanoTime();
            HeadersWrapper hw;
            try {
                hw = downloadedHeaders.take();
            } catch (InterruptedException e) {
                duration = System.nanoTime() - startTime;
                surveyLog.info("TaskGetBodies: wait for headers, duration = {} ns.", duration);
                continue;
            }
            duration = System.nanoTime() - startTime;
            surveyLog.info("TaskGetBodies: wait for headers, duration = {} ns.", duration);

            startTime = System.nanoTime();
            int idHash = hw.getNodeIdHash();
            String displayId = hw.getDisplayId();
            List<BlockHeader> headers = hw.getHeaders();
            if (headers.isEmpty()) {
                duration = System.nanoTime() - startTime;
                surveyLog.info("TaskGetBodies: make request, duration = {} ns.", duration);
                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug(
                        "<get-bodies from-num={} to-num={} node={}>",
                        headers.get(0).getNumber(),
                        headers.get(headers.size() - 1).getNumber(),
                        hw.getDisplayId());
            }

            p2p.send(
                    idHash,
                    displayId,
                    new ReqBlocksBodies(
                            headers.stream().map(k -> k.getHash()).collect(Collectors.toList())));
            stats.updateTotalRequestsToPeer(displayId, RequestType.BODIES);
            stats.updateRequestTime(displayId, System.nanoTime(), RequestType.BODIES);

            // save headers for matching with bodies
            syncRequestManager.storeHeaders(idHash, hw);

            duration = System.nanoTime() - startTime;
            surveyLog.info("TaskGetBodies: make request, duration = {} ns.", duration);
        }
    }
}
