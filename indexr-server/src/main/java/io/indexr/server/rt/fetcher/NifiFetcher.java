package io.indexr.server.rt.fetcher;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.Transaction.TransactionState;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.protocol.DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.Fetcher;
import io.indexr.segment.rt.UTF8JsonRowCreator;
import io.indexr.segment.rt.UTF8Row;
import io.indexr.util.JsonUtil;

public class NifiFetcher implements Fetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(NifiFetcher.class);

    private final SiteToSiteClient.Builder siteToSiteClientBuilder;
    private final UTF8JsonRowCreator utf8JsonRowCreator;

    private SiteToSiteClient siteToSiteClient;
    private Transaction transaction;

    @JsonProperty("number.empty.as.zero")
    public boolean numberEmptyAsZero;

    @JsonProperty("properties")
    public final Properties properties;

    @JsonCreator
    public NifiFetcher(@JsonProperty("number.empty.as.zero") Boolean numberEmptyAsZero,
            @JsonProperty("properties") Properties properties) {
        this.properties = properties;
        String urlString = properties.getProperty("nifi.connection.url");
        String portName = properties.getProperty("nifi.connection.portName");
        String requestBatchCount = properties.getProperty("nifi.connection.requestBatchCount");
        this.siteToSiteClientBuilder = new SiteToSiteClient.Builder();
        this.siteToSiteClientBuilder.url(urlString);
        this.siteToSiteClientBuilder.portName(portName);
        this.siteToSiteClientBuilder.requestBatchCount(Integer.valueOf(requestBatchCount));
        this.utf8JsonRowCreator = new UTF8JsonRowCreator(this.numberEmptyAsZero);
    }

    @Override
    public void setRowCreator(String name, UTF8Row.Creator rowCreator) {
        utf8JsonRowCreator.setRowCreator(name, rowCreator);
    }

    @Override
    public synchronized boolean ensure(SegmentSchema schema) {
        if (transaction != null) {
            try {
                TransactionState state = transaction.getState();
                LOGGER.debug("Stale NiFi transaction in {} state found", state);
                if (state == TransactionState.TRANSACTION_STARTED || state == TransactionState.DATA_EXCHANGED) {
                    return true;
                } else {
                    cancelTransaction("Wrong transaction state " + state, null);
                }
            } catch (IOException e) {
                cancelTransaction("Cannot get state of the transaction", e);
            }
        }
        if (siteToSiteClient == null) {
            siteToSiteClient = siteToSiteClientBuilder.build();
        }
        return transaction == null ? createTransaction() : true;
    }

    @Override
    public synchronized boolean hasNext() {
        return transaction == null ? createTransaction() : true;
    }

    /**
     * Blocking call, similar to Kafka or Console Fetcher, otherwise we create too many tables.
     * 
     * @throws InterruptedException
     *             when interrupted
     * 
     * @see io.indexr.segment.rt.Fetcher#next()
     */
    @Override
    public List<UTF8Row> next() throws InterruptedException {
        byte[] data = null;
        while (true) {
            synchronized (this) {
                DataPacket packet = null;
                if (transaction == null && !createTransaction()) {
                    return Collections.emptyList();
                }
                try { // now transaction cannot be null
                    packet = transaction.receive();
                    if (packet != null) {
                        data = IOUtils.toByteArray(packet.getData(), packet.getSize()); // we cannot close the stream
                        break;
                    }
                } catch (IOException | TransmissionDisabledException e) {
                    cancelTransaction("Cannot receive packet from transaction", e);
                    return Collections.emptyList();
                }

                try { // no new packet received
                    transaction.confirm();
                    transaction.complete();
                    transaction = null;
                } catch (IOException e) {
                    cancelTransaction("Cannot commit transaction", e);
                    return Collections.emptyList();
                }
            }
            Thread.sleep(100);
        }
        // finally we have some data to process
        try {
            return utf8JsonRowCreator.create(data);
        } catch (Exception e) {
            LOGGER.debug("Illegal data", e);
        }
        return Collections.emptyList();
    }

    private boolean createTransaction() {
        if (siteToSiteClient == null) {
            return false;
        }
        try {
            transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
            return true;
        } catch (IOException e) {
            LOGGER.error("Cannot create transaction", e);
            return false;
        }
    }

    private void cancelTransaction(String msg, Exception ex) {
        if (transaction == null) {
            return;
        }
        LOGGER.error(msg, ex);
        try {
            transaction.cancel(msg);
        } catch (Exception e) {
            LOGGER.debug("Cannot cancel transaction", e);
        }
        transaction = null;
    }

    @Override
    public void commit() {
        // no op, as we auto commit transactions inside next() method, similar to kafka
    }

    @Override
    public synchronized void close() throws IOException {
        cancelTransaction("Closing the Fetcher", null);
        IOUtils.closeQuietly(this.siteToSiteClient);
        this.siteToSiteClient = null;
    }

    @Override
    public String toString() {
        String settings = JsonUtil.toJson(this);
        return String.format("Nifi fetcher: %s", settings);
    }

    @Override
    public boolean equals(Fetcher o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NifiFetcher that = (NifiFetcher) o;
        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public long statConsume() {
        return utf8JsonRowCreator.getConsumeCount();
    }

    @Override
    public long statProduce() {
        return utf8JsonRowCreator.getProduceCount();
    }

    @Override
    public long statIgnore() {
        return utf8JsonRowCreator.getIgnoreCount();
    }

    @Override
    public long statFail() {
        return utf8JsonRowCreator.getFailCount();
    }

    @Override
    public void statReset() {
        utf8JsonRowCreator.resetStat();
    }
}
