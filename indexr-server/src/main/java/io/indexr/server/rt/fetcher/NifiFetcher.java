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
                    try {
                        transaction.cancel("Wrong transaction state " + state);
                    } catch (IOException e) {
                    }
                    transaction = null;
                }
            } catch (IOException e) {
                LOGGER.error("Cannot get state of the transaction, closing client", e);
                IOUtils.closeQuietly(siteToSiteClient);
                siteToSiteClient = null;
                transaction = null;
            }
        }
        if (siteToSiteClient == null) {
            siteToSiteClient = siteToSiteClientBuilder.build();
        }
        if (transaction == null) {
            try {
                transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
            } catch (IOException e) {
                LOGGER.error("Cannot create transaction", transaction);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasNext() throws IOException {
        return true;
    }

    /**
     * Blocking call, similar to Kafka or Console Fetcher, otherwise we create too
     * many tables.
     * 
     * @see io.indexr.segment.rt.Fetcher#next()
     */
    @Override
    public List<UTF8Row> next() throws Exception {
        DataPacket packet = null;
        while (true) {
            if (siteToSiteClient == null) {
                return Collections.emptyList();
            }
            if (transaction == null) {
                transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
            }
            packet = transaction.receive();

            if (packet != null) {
                break; // parse it
            } else if (transaction != null) {
                transaction.confirm();
                transaction.complete();
                transaction = null;
            }
            Thread.sleep(100);
        }
        return parseUTF8Row(packet);
    }

    private List<UTF8Row> parseUTF8Row(DataPacket packet) {
        try {
            byte[] data = IOUtils.toByteArray(packet.getData(), packet.getSize()); // we cannot close the stream
            return utf8JsonRowCreator.create(data);
        } catch (Exception e) {
            LOGGER.debug("Illegal data", e);
            return Collections.emptyList();
        }
    }

    @Override
    public void commit() {
        // no op, as we auto commit transactions inside next() method, similar to kafka
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.transaction != null) {
            try {
                this.transaction.cancel("Closing the Fetcher");
            } catch (Exception e) {
                LOGGER.error("Cannot cancel transaction", e);
            }
            this.transaction = null;
        }
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
