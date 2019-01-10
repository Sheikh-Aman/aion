package org.aion.p2p;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ThreadLocalRandom;
import org.junit.Before;
import org.junit.Test;

public class HeaderTest {

    private short version;
    private byte ctl;
    private byte action;
    private int length;
    private Header hd;
    private int route;

    @Before
    public void setup() {
        version = (short) ThreadLocalRandom.current().nextInt();
        System.out.println("Version: " + version);
        ctl = 0;
        action = 4;
        length = 8;
        hd = new Header(version, ctl, action, length);
        route = (version << 16) | (ctl << 8) | action;
    }

    @Test
    public void testHeader() {
        assertEquals(version, hd.getVer());
        assertEquals(ctl, hd.getCtrl());
        assertEquals(action, hd.getAction());
        assertEquals(length, hd.getLen());
        assertEquals(route, hd.getRoute());
    }

    @Test
    public void testHeaderLen() {
        hd.setLen(40);
        assertEquals(40, hd.getLen());
    }

    @Test
    public void encodeDecode() {
        byte[] bytes = hd.encode();
        Header hdr = Header.decode(bytes);
        assertEquals(version, hdr.getVer());
        assertEquals(ctl, hd.getCtrl());
        assertEquals(action, hd.getAction());
        assertEquals(length, hd.getLen());
        assertEquals(route, hd.getRoute());
    }

    @Test
    public void encodeDecode2() {
        hd.setLen(P2pConstant.MAX_BODY_SIZE);
        byte[] bytes = hd.encode();
        Header hdr = Header.decode(bytes);
        assertEquals(version, hdr.getVer());
        assertEquals(ctl, hd.getCtrl());
        assertEquals(action, hd.getAction());
        assertEquals(P2pConstant.MAX_BODY_SIZE, hd.getLen());
        assertEquals(route, hd.getRoute());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void encodeDecode3() {
        hd.setLen(P2pConstant.MAX_BODY_SIZE + 1);
        byte[] bytes = hd.encode();
        Header.decode(bytes);
    }

    @Test
    public void repeatEncodeDecode() {
        for (int i = 0; i < 100; i++) {
            encodeDecode();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void decodeThrow() {
        Header.decode(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void decodeThrow2() {
        byte[] data = new byte[length - 1];
        Header.decode(data);
    }
}
