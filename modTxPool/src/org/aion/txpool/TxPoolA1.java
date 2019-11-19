package org.aion.txpool;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.aion.base.AionTransaction;
import org.aion.base.PooledTransaction;
import org.aion.log.AionLoggerFactory;
import org.aion.log.LogEnum;
import org.aion.types.AionAddress;
import org.aion.util.types.ByteArrayWrapper;
import org.slf4j.Logger;

public class TxPoolA1 implements ITxPool{

    private static final Logger LOG = AionLoggerFactory.getLogger(LogEnum.TXPOOL.toString());

    private int txn_timeout = 3600; // 1 hour
    private int blkSizeLimit = Constant.MAX_BLK_SIZE; // 2MB
    final static long MIN_ENERGY_CONSUME = 21_000L;

    private final AtomicLong blkNrgLimit = new AtomicLong(10_000_000L);
    private final int multiplyM = 1_000_000;
    private final int TXN_TIMEOUT_MIN = 10; // 10s

    private final int BLK_SIZE_MAX = 16 * 1024 * 1024; // 16MB
    private final int BLK_SIZE_MIN = 1024 * 1024; // 1MB

    private final int BLK_NRG_MAX = 100_000_000;
    private final int BLK_NRG_MIN = 1_000_000;

    private final int MAX_POOL_SIZE = 8192;

    /**
     * mainMap : Map<ByteArrayWrapper, TXState> @ByteArrayWrapper transaction hash @TXState
     * transaction data and sort status
     */
    // TODO : should limit size
    private final Map<ByteArrayWrapper, PooledTransaction> mainMap = new ConcurrentHashMap<>(MAX_POOL_SIZE);
    /**
     * timeView : SortedMap<Long, LinkedHashSet<ByteArrayWrapper>> @Long transaction
     * timestamp @LinkedHashSet<ByteArrayWrapper> the hashSet of the transaction hash*
     */
    private final SortedMap<Long, Set<ByteArrayWrapper>> timeView =
        Collections.synchronizedSortedMap(new TreeMap<>());
    /**
     * feeView : SortedMap<BigInteger, LinkedHashSet<TxPoolList<ByteArrayWrapper>>> @BigInteger
     * energy cost = energy consumption * energy price @LinkedHashSet<TxPoolList<ByteArrayWrapper>>
     * the TxPoolList of the first transaction hash
     */
    private final SortedMap<Long, Set<ByteArrayWrapper>>
        feeView = Collections.synchronizedSortedMap(new TreeMap<>());

    /**
     * accountView : Map<AionAddress, Map<BigInteger, ByteArrayWrapper>>
     * @AionAddress account
     * @BigInteger Transaction nonce
     * @ByteArrayWrapper TransactionHash
     */
    private final Map<AionAddress, Map<BigInteger, ByteArrayWrapper>> accountView = new ConcurrentHashMap<>();

    private final List<PooledTransaction> outDatedTransactions = new ArrayList<>();


//    /**
//     * poolStateView : Map<ByteArrayWrapper, List<PoolState>> @ByteArrayWrapper account
//     * address @PoolState continuous transaction state including starting nonce
//     */
//    private final Map<AionAddress, List<PoolState>> poolStateView = new ConcurrentHashMap<>();

    public TxPoolA1(Properties config) {
        setPoolArgs(config);
    }

    private void setPoolArgs(Properties config) {
        if (Optional.ofNullable(config.get(PROP_TX_TIMEOUT)).isPresent()) {
            txn_timeout = Integer.valueOf(config.get(PROP_TX_TIMEOUT).toString());
            if (txn_timeout < TXN_TIMEOUT_MIN) {
                txn_timeout = TXN_TIMEOUT_MIN;
            }
        }

        txn_timeout--; // final timeout value sub -1 sec

        if (Optional.ofNullable(config.get(PROP_BLOCK_SIZE_LIMIT)).isPresent()) {
            blkSizeLimit = Integer.valueOf(config.get(PROP_BLOCK_SIZE_LIMIT).toString());
            if (blkSizeLimit < BLK_SIZE_MIN) {
                blkSizeLimit = BLK_SIZE_MIN;
            } else if (blkSizeLimit > BLK_SIZE_MAX) {
                blkSizeLimit = BLK_SIZE_MAX;
            }
        }

        if (Optional.ofNullable(config.get(PROP_BLOCK_NRG_LIMIT)).isPresent()) {
            updateBlkNrgLimit(Long.valueOf((String) config.get(PROP_BLOCK_NRG_LIMIT)));
        }
    }

    @Override
    public List<PooledTransaction> add(List<PooledTransaction> list) {

        if (list == null || list.isEmpty()) {
            return new ArrayList<>();
        }

        if (mainMap.size() == MAX_POOL_SIZE) {
            LOG.warn("TxPool is full. No transaction has been added!");
            return new ArrayList<>();
        }

        for (PooledTransaction poolTx : list) {
            ByteArrayWrapper hashWrapper = ByteArrayWrapper.wrap(poolTx.tx.getTransactionHash());
            PooledTransaction p = mainMap.putIfAbsent(hashWrapper, poolTx);
            if (p == null) {    // new TX
                long txTime = poolTx.tx.getTimeStampBI().longValue() / multiplyM;
                Set<ByteArrayWrapper> timeSet = timeView.get(txTime);
                if (timeSet == null) {
                    Set<ByteArrayWrapper> newTimeSet = new LinkedHashSet<>();
                    newTimeSet.add(hashWrapper);
                    timeView.put(txTime, newTimeSet);
                } else {
                    timeSet.add(hashWrapper);
                    timeView.put(txTime, timeSet);
                }

                long txEnergyPrice = poolTx.tx.getEnergyPrice();
                Set<ByteArrayWrapper> feeSet = feeView.get(txEnergyPrice);
                if (feeSet == null) {
                    Set<ByteArrayWrapper> newFeeSet = new LinkedHashSet<>();
                    newFeeSet.add(hashWrapper);
                    feeView.put(txEnergyPrice, newFeeSet);
                } else {
                    feeSet.add(hashWrapper);
                    feeView.put(txEnergyPrice, feeSet);
                }




            } else {
                LOG.debug("TxPool has the same transaction");
            }
        }

        return null;
    }

    @Override
    public PooledTransaction add(PooledTransaction tx) {
        List<PooledTransaction> rtn = this.add(Collections.singletonList(tx));
        return rtn.isEmpty() ? null : rtn.get(0);
    }

    @Override
    public List<PooledTransaction> remove(List<PooledTransaction> tx) {
        return null;
    }

    @Override
    public PooledTransaction remove(PooledTransaction tx) {
        return null;
    }

    @Override
    public List<PooledTransaction> removeTxsWithNonceLessThan(
        Map<AionAddress, BigInteger> accNonce) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public List<AionTransaction> snapshot() {
        return null;
    }

    @Override
    public List<PooledTransaction> getOutdatedList() {
        return null;
    }

    @Override
    public long getOutDateTime() {
        return 0;
    }

    @Override
    public BigInteger bestPoolNonce(AionAddress addr) {
        return null;
    }

    @Override
    public void updateBlkNrgLimit(long nrg) {

    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public List<AionTransaction> snapshotAll() {
        return null;
    }

    @Override
    public PooledTransaction getPoolTx(AionAddress from, BigInteger txNonce) {
        return null;
    }
}
