package org.aion.p2p.impl1;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.aion.p2p.Ctrl;
import org.aion.p2p.Handler;
import org.aion.p2p.Header;
import org.aion.p2p.INode;
import org.aion.p2p.INodeMgr;
import org.aion.p2p.IP2pMgr;
import org.aion.p2p.Msg;
import org.aion.p2p.P2pConstant;
import org.aion.p2p.Ver;
import org.aion.p2p.impl.TaskRequestActiveNodes;
import org.aion.p2p.impl.TaskUPnPManager;
import org.aion.p2p.impl.comm.Node;
import org.aion.p2p.impl.comm.NodeMgr;
import org.aion.p2p.impl.zero.msg.ReqHandshake1;
import org.aion.p2p.impl.zero.msg.ResHandshake1;
import org.aion.p2p.impl1.tasks.MsgOut;
import org.aion.p2p.impl1.tasks.TaskClear;
import org.aion.p2p.impl1.tasks.TaskConnectPeers;
import org.aion.p2p.impl1.tasks.TaskInbound;
import org.aion.p2p.impl1.tasks.TaskSend;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;

/** @author Chris p2p://{uuid}@{ip}:{port} */
public final class P2pMgr implements IP2pMgr {
    private static final int PERIOD_SHOW_STATUS = 10000;
    private static final int PERIOD_REQUEST_ACTIVE_NODES = 1000;
    private static final int PERIOD_UPNP_PORT_MAPPING = 3600000;
    private static final int TIMEOUT_MSG_READ = 10000;

    // TODO: need refactor by passing the parameter in the later version to P2pMgr.
    public static int txBroadCastRoute =
            (Ctrl.SYNC << 8) + 6; // ((Ver.V0 << 16) + (Ctrl.SYNC << 8) + 6);

    public final Logger p2pLOG, surveyLog;

    public static final int WORKER = 32;
    private final int SOCKET_RECV_BUFFER = 1024 * 128;
    private final int SOCKET_BACKLOG = 1024;

    private final int maxTempNodes, maxActiveNodes, selfNodeIdHash, selfPort;
    private final int selfChainId;
    private boolean syncSeedsOnly, upnpEnable;
    private String selfRevision, selfShortId;
    private final byte[] selfNodeId, selfIp;
    private INodeMgr nodeMgr;
    private final Map<Integer, List<Handler>> handlers = new ConcurrentHashMap<>();
    private final Set<Short> versions = new HashSet<>();
    private final Map<Integer, Integer> errCnt = Collections.synchronizedMap(new LRUMap<>(128));
    private final AtomicBoolean start = new AtomicBoolean(true);

    private ServerSocketChannel tcpServer;
    private Selector selector;
    private ScheduledExecutorService scheduledWorkers;
    private int errTolerance;

    private static ReqHandshake1 cachedReqHandshake1;
    private static ResHandshake1 cachedResHandshake1;

    private ExecutorService workersReceive =
        Executors.newCachedThreadPool(
            new ThreadFactory() {

                private AtomicInteger cnt = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "receive-" + cnt.incrementAndGet());
                }
            });

    private ExecutorService workersSend =
        Executors.newCachedThreadPool(
            new ThreadFactory() {

                private AtomicInteger cnt = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "send-" + cnt.incrementAndGet());
                }
            });

    @Override
    public void submitTask(Runnable task) {
        if (!workersReceive.isShutdown()) {
            workersReceive.submit(task);
        }
    }

    public enum Dest {
        INBOUND,
        OUTBOUND,
        ACTIVE
    }

    /**
     * @param chainId identifier assigned to the current chain read from the blockchain
     *     configuration. Peer connections are allowed only for equal network identifiers.
     * @param _nodeId byte[36]
     * @param _ip String
     * @param _port int
     * @param _bootNodes String[]
     * @param _upnpEnable boolean
     * @param _maxTempNodes int
     * @param _maxActiveNodes int
     */
    public P2pMgr(
            final Logger _p2pLog,
            final Logger surveyLog,
            final int chainId,
            final String _revision,
            final String _nodeId,
            final String _ip,
            final int _port,
            final String[] _bootNodes,
            final boolean _upnpEnable,
            final int _maxTempNodes,
            final int _maxActiveNodes,
            final boolean _bootlistSyncOnly,
            final int _errorTolerance) {

        if (_p2pLog == null) {
            throw new NullPointerException("A non-null logger must be provided in the constructor.");
        }
        this.p2pLOG = _p2pLog;
        this.surveyLog = surveyLog;
        this.selfChainId = chainId;
        this.selfRevision = _revision;
        this.selfNodeId = _nodeId.getBytes();
        this.selfNodeIdHash = Arrays.hashCode(selfNodeId);
        this.selfShortId = new String(Arrays.copyOfRange(_nodeId.getBytes(), 0, 6));
        this.selfIp = Node.ipStrToBytes(_ip);
        this.selfPort = _port;
        this.upnpEnable = _upnpEnable;
        this.maxTempNodes = _maxTempNodes;
        this.maxActiveNodes = _maxActiveNodes;
        this.syncSeedsOnly = _bootlistSyncOnly;
        this.errTolerance = _errorTolerance;

        nodeMgr = new NodeMgr(this, _maxActiveNodes, _maxTempNodes, p2pLOG);

        for (String _bootNode : _bootNodes) {
            Node node = Node.parseP2p(_bootNode);
            if (validateNode(node)) {
                try {
                    nodeMgr.addTempNode(node);
                } catch (InterruptedException e) {
                    p2pLOG.error("p2pMgr construct InterruptedException! " + node.toString(), e);
                    continue;
                }
                nodeMgr.seedIpAdd(node.getIpStr());
            }
        }

        // rem out for bug:
        // nodeMgr.loadPersistedNodes();
        cachedResHandshake1 = new ResHandshake1(p2pLOG, true, this.selfRevision);
    }

    @Override
    public void run() {
        try {
            selector = Selector.open();

            scheduledWorkers = new ScheduledThreadPoolExecutor(2);

            tcpServer = ServerSocketChannel.open();
            tcpServer.configureBlocking(false);
            tcpServer.socket().setReuseAddress(true);
            /*
             * Bigger RECV_BUFFER and BACKLOG can have a better socket read/write tolerance, can be a advanced p2p settings in the config file.
             */
            tcpServer.socket().setReceiveBufferSize(SOCKET_RECV_BUFFER);

            try {
                tcpServer
                        .socket()
                        .bind(
                                new InetSocketAddress(Node.ipBytesToStr(selfIp), selfPort),
                                SOCKET_BACKLOG);
            } catch (IOException e) {
                p2pLOG.error(
                        "Failed to connect to Socket Address: "
                                + Node.ipBytesToStr(selfIp)
                                + ":"
                                + selfPort
                                + ", please check your ip and port configration!",
                        e);
            }

            tcpServer.register(selector, SelectionKey.OP_ACCEPT);

            Thread thrdIn = new Thread(getInboundInstance(), "p2p-in");
            thrdIn.setPriority(Thread.NORM_PRIORITY);
            thrdIn.start();

            if (p2pLOG.isDebugEnabled()) {
                this.handlers.forEach(
                        (route, callbacks) -> {
                            Handler handler = callbacks.get(0);
                            Header h = handler.getHeader();
                            p2pLOG.debug(
                                    "handler route={} v-c-a={}-{}-{} name={}",
                                    route,
                                    h.getVer(),
                                    h.getCtrl(),
                                    h.getAction(),
                                    handler.getClass().getSimpleName());
                        });
            }

            if (upnpEnable) {
                scheduledWorkers.scheduleWithFixedDelay(
                        new TaskUPnPManager(p2pLOG, selfPort),
                        1,
                        PERIOD_UPNP_PORT_MAPPING,
                        TimeUnit.MILLISECONDS);
            }

//            if (p2pLOG.isInfoEnabled()) {
//                Thread threadStatus = new Thread(getStatusInstance(), "p2p-ts");
//                threadStatus.setPriority(Thread.NORM_PRIORITY);
//                threadStatus.start();
//            }

            if (!syncSeedsOnly) {
                scheduledWorkers.scheduleWithFixedDelay(
                        new TaskRequestActiveNodes(this, p2pLOG),
                        5000,
                        PERIOD_REQUEST_ACTIVE_NODES,
                        TimeUnit.MILLISECONDS);
            }

            Thread thrdClear = new Thread(getClearInstance(), "p2p-clear");
            thrdClear.setPriority(Thread.NORM_PRIORITY);
            thrdClear.start();

            Thread thrdConn = new Thread(getConnectPeersInstance(), "p2p-conn");
            thrdConn.setPriority(Thread.NORM_PRIORITY);
            thrdConn.start();
        } catch (SocketException e) {
            p2pLOG.error("tcp-server-socket-exception.", e);
        } catch (IOException e) {
            p2pLOG.error("tcp-server-io-exception.", e);
        }
    }

    @Override
    public void register(final List<Handler> _cbs) {
        for (Handler _cb : _cbs) {
            Header h = _cb.getHeader();
            short ver = h.getVer();
            byte ctrl = h.getCtrl();
            if (Ver.filter(ver) != Ver.UNKNOWN && Ctrl.filter(ctrl) != Ctrl.UNKNOWN) {
                versions.add(ver);

                int route = h.getRoute();
                List<Handler> routeHandlers = handlers.get(route);
                if (routeHandlers == null) {
                    routeHandlers = new ArrayList<>();
                    routeHandlers.add(_cb);
                    handlers.put(route, routeHandlers);
                } else {
                    routeHandlers.add(_cb);
                }
            }
        }

        List<Short> supportedVersions = new ArrayList<>(versions);
        cachedReqHandshake1 = getReqHandshake1Instance(supportedVersions);
    }

    @Override
    public void send(int _nodeIdHash, String _nodeIdShort, final Msg _msg) {
        if (!workersSend.isShutdown()) {
            workersSend.submit(
                    new TaskSend(
                            p2pLOG,
                            surveyLog,
                            this,
                            new MsgOut(_nodeIdHash, _nodeIdShort, _msg, Dest.ACTIVE),
                            nodeMgr,
                            selector));
        }
    }

    public void send(int _nodeIdHash, String _nodeIdShort, final Msg _msg, Dest dest) {
        if (!workersSend.isShutdown()) {
            workersSend.submit(
                new TaskSend(
                    p2pLOG,
                    surveyLog,
                    this,
                    new MsgOut(_nodeIdHash, _nodeIdShort, _msg, dest),
                    nodeMgr,
                    selector));
        }
    }

    private static final int THREAD_Q_LIMIT = 20000;

    ThreadPoolExecutor tpe =
            new ThreadPoolExecutor(
                    WORKER,
                    WORKER,
                    0,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(THREAD_Q_LIMIT),
                    Executors.defaultThreadFactory());

    @Override
    public void executeTpe(Runnable task) {
        if (!tpe.isShutdown()) {
            tpe.execute(task);
        }
    }

    @Override
    public void shutdown() {
        start.set(false);
        workersReceive.shutdown();
        workersSend.shutdown();
        if (scheduledWorkers != null) {
            scheduledWorkers.shutdownNow();
        }

        for (List<Handler> hdrs : handlers.values()) {
            hdrs.forEach(Handler::shutDown);
        }
        nodeMgr.shutdown();
    }

    @Override
    public List<Short> versions() {
        return new ArrayList<>(versions);
    }

    @Override
    public void errCheck(int _nodeIdHash, String _displayId) {
        int cnt = (errCnt.get(_nodeIdHash) == null ? 1 : (errCnt.get(_nodeIdHash) + 1));
        if (cnt > this.errTolerance) {
            ban(_nodeIdHash);
            errCnt.put(_nodeIdHash, 0);

            if (p2pLOG.isDebugEnabled()) {
                p2pLOG.debug(
                        "ban node={} err-count={}",
                        (_displayId == null ? _nodeIdHash : _displayId),
                        cnt);
            }
        } else {
            errCnt.put(_nodeIdHash, cnt);
        }
    }

    /** @param _sc SocketChannel */
    public void closeSocket(final SocketChannel _sc, String _reason) {
        closeSocket(_sc, _reason, null);
    }

    @Override
    public void closeSocket(SocketChannel _sc, String _reason, Exception e) {
        if (p2pLOG.isDebugEnabled()) {
            if (e != null) {
                p2pLOG.debug("close-socket reason=" + _reason, e);
            } else {
                p2pLOG.debug("close-socket reason={}", _reason);
            }
        }

        if (_sc != null) {
            SelectionKey sk = _sc.keyFor(selector);
            if (sk != null) {
                sk.cancel();
                sk.attach(null);
            }

            try {
                _sc.close();
            } catch (IOException ex) {
                p2pLOG.info("close-socket-io-exception.", ex);
            }
        }
    }

    /**
     * Remove an active node if exists.
     *
     * @param _nodeIdHash int
     * @param _reason String
     */
    @Override
    public void dropActive(int _nodeIdHash, String _reason) {
        nodeMgr.dropActive(_nodeIdHash, _reason);
    }

    /**
     * @param _node Node
     * @return boolean
     */
    @Override
    public boolean validateNode(final INode _node) {
        if (_node != null) {
            boolean notSelfId = !Arrays.equals(_node.getId(), this.selfNodeId);
            boolean notSameIpOrPort =
                    !(Arrays.equals(selfIp, _node.getIp()) && selfPort == _node.getPort());
            boolean notActive = nodeMgr.notActiveNode(_node.getPeerId());
            boolean notOutbound = nodeMgr.notAtOutboundList(_node.getPeerId());
            return notSelfId && notSameIpOrPort && notActive && notOutbound;
        } else {
            return false;
        }
    }

    /** @param _channel SocketChannel TODO: check option */
    @Override
    public void configChannel(final SocketChannel _channel) throws IOException {
        _channel.configureBlocking(false);
        _channel.socket().setSoTimeout(TIMEOUT_MSG_READ);
        _channel.socket().setReceiveBufferSize(P2pConstant.RECV_BUFFER_SIZE);
        _channel.socket().setSendBufferSize(P2pConstant.SEND_BUFFER_SIZE);
    }

    private void ban(int nodeIdHashcode) {
        nodeMgr.ban(nodeIdHashcode);
        nodeMgr.dropActive(nodeIdHashcode, "ban");
    }

    // <------------------------ getter methods below --------------------------->

    @Override
    public INode getRandom() {
        return this.nodeMgr.getRandom();
    }

    @Override
    public Map<Integer, INode> getActiveNodes() {
        return this.nodeMgr.getActiveNodesMap();
    }

    public int getTempNodesCount() {
        return this.nodeMgr.tempNodesSize();
    }

    @Override
    public int getMaxActiveNodes() {
        return this.maxActiveNodes;
    }

    @Override
    public int getMaxTempNodes() {
        return this.maxTempNodes;
    }

    @Override
    public boolean isSyncSeedsOnly() {
        return this.syncSeedsOnly;
    }

    @Override
    public int getAvgLatency() {
        return this.nodeMgr.getAvgLatency();
    }

    @Override
    public boolean isCorrectNetwork(int netId){
        return netId == selfChainId;
    }

    /**
     * @implNote Compares the port and id to the given node to allow connections to the same id and
     *     different port. Does not compare IP values since the self IP is often recorded as 0.0.0.0
     *     in the configuration file and cannot be inferred reliably by the node itself.
     */
    @Override
    public boolean isSelf(INode node) {
        return selfNodeIdHash == node.getIdHash()
                && selfPort == node.getPort()
                && Arrays.equals(selfNodeId, node.getId());
    }

    private TaskInbound getInboundInstance() {
        return new TaskInbound(
                p2pLOG,
                surveyLog,
                this,
                this.selector,
                this.start,
                this.nodeMgr,
                this.handlers,
                cachedResHandshake1);
    }

//    private TaskStatus getStatusInstance() {
//        return new TaskStatus(p2pLOG, surveyLog, start, nodeMgr, selfShortId, sendMsgQue, receiveMsgQue);
//    }

    private TaskClear getClearInstance() {
        return new TaskClear(p2pLOG, nodeMgr, start);
    }

    private TaskConnectPeers getConnectPeersInstance() {
        return new TaskConnectPeers(
                p2pLOG,
                this,
                this.start,
                this.nodeMgr,
                this.maxActiveNodes,
                this.selector,
                cachedReqHandshake1);
    }

    private ReqHandshake1 getReqHandshake1Instance(List<Short> versions) {
        return new ReqHandshake1(
                selfNodeId,
                selfChainId,
                this.selfIp,
                this.selfPort,
                this.selfRevision.getBytes(),
                versions);
    }
}
