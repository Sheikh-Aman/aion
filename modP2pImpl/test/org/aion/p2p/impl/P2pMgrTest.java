package org.aion.p2p.impl;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.UUID;
import org.aion.p2p.impl1.P2pMgr;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

/** @author chris */
public class P2pMgrTest {

    @Mock
    private Logger p2pLOG;

    private String nodeId1 = UUID.randomUUID().toString();
    private String nodeId2 = UUID.randomUUID().toString();
    private String ip1 = "127.0.0.1";
    private String ip2 = "192.168.0.11";
    private int port1 = 30303;
    private int port2 = 30304;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    public Map.Entry<P2pMgr, P2pMgr> newTwoNodeSetup() {
        String ip = "127.0.0.1";

        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();

        int port1 = 30303;
        int port2 = 30304;

        // we want node 1 to connect to node 2
        String[] nodes = new String[] {"p2p://" + id2 + "@" + ip + ":" + port2};

        // to guarantee they don't receive the same port

        System.out.println("connector on: " + TestUtilities.formatAddr(id1, ip, port1));
        P2pMgr connector = new P2pMgr(p2pLOG, p2pLOG,0, "", id1, ip, port1, nodes, false, 128, 128, false, 50);

        System.out.println("receiver on: " + TestUtilities.formatAddr(id2, ip, port2));
        P2pMgr receiver =
                new P2pMgr(p2pLOG,p2pLOG, 0, "", id2, ip, port2, new String[0], false, 128, 128, false, 50);

        return Map.entry(connector, receiver);
    }

    @Test
    public void testIgnoreSameNodeIdAsSelf() {

        String[] nodes = new String[] {"p2p://" + nodeId1 + "@" + ip2 + ":" + port2};

        P2pMgr p2p = new P2pMgr(p2pLOG, p2pLOG,0, "", nodeId1, ip1, port1, nodes, false, 128, 128, false, 50);

        assertEquals(p2p.getTempNodesCount(), 0);
    }

    @Test
    public void testIgnoreSameIpAndPortAsSelf() {

        String[] nodes = new String[] {"p2p://" + nodeId2 + "@" + ip1 + ":" + port1};

        P2pMgr p2p = new P2pMgr(p2pLOG, p2pLOG,0, "", nodeId1, ip1, port1, nodes, false, 128, 128, false, 50);
        assertEquals(0, p2p.getTempNodesCount());
    }

    @Test
    public void testTempNodes() {

        String[] nodes =
                new String[] {
                    "p2p://" + nodeId2 + "@" + ip1 + ":" + port2,
                    "p2p://" + nodeId2 + "@" + ip2 + ":" + port1,
                    "p2p://" + nodeId2 + "@" + ip2 + ":" + port2,
                };

        P2pMgr p2p = new P2pMgr(p2pLOG, p2pLOG,0, "", nodeId1, ip1, port1, nodes, false, 128, 128, false, 50);
        assertEquals(p2p.getTempNodesCount(), 3);
    }
}
