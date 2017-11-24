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
    private DataPacket dataPacket;

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
                if (state == TransactionState.TRANSACTION_STARTED || state == TransactionState.DATA_EXCHANGED
                        || state == TransactionState.TRANSACTION_CONFIRMED) {
                    return hasNext();
                } else {
                    transaction = null;
                    dataPacket = null;
                }
            } catch (IOException e) {
                LOGGER.error("Cannot get state of the transaction, closing client", e);
                IOUtils.closeQuietly(siteToSiteClient);
                siteToSiteClient = null;
                transaction = null;
                dataPacket = null;
            }
        }
        if (siteToSiteClient == null) {
            siteToSiteClient = siteToSiteClientBuilder.build();
        }
        try {
            transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
            return hasNext();
        } catch (Exception e) {
            LOGGER.error("Cannot create or read transaction", e);
            return false;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (this.dataPacket == null && this.transaction != null) {
            this.dataPacket = this.transaction.receive();
        }
        return this.dataPacket != null;
    }

    @Override
    public List<UTF8Row> next() throws IOException {
        DataPacket packet = dataPacket;
        dataPacket = null;
        return parseUTF8Row(readDataPacket(packet));
    }

    private byte[] readDataPacket(DataPacket packet) throws IOException {
        return IOUtils.toByteArray(packet.getData(), packet.getSize()); // we cannot close the stream
    }

    private List<UTF8Row> parseUTF8Row(byte[] data) {
        try {
            return utf8JsonRowCreator.create(data);
        } catch (Exception e) {
            LOGGER.debug("Illegal data", e);
            return Collections.emptyList();
        }
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
            this.dataPacket = null;
        }
        IOUtils.closeQuietly(this.siteToSiteClient);
        this.siteToSiteClient = null;
    }

    @Override
    public void commit() {
        // commit only in case we have fully read the transaction
        try {
            if (!hasNext()) {
                try {
                    transaction.confirm();
                    transaction.complete();
                } catch (IOException e) {
                    LOGGER.error("Cannot commit transaction", e);
                }
                transaction = null;
                dataPacket = null;
            }
        } catch (IOException e) {
            LOGGER.warn("Cannot read from transaction", e);
        }
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
