package io.indexr.server.rt.fetcher;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rt.Fetcher;
import io.indexr.segment.rt.UTF8JsonRowCreator;
import io.indexr.segment.rt.UTF8Row;
import io.indexr.util.JsonUtil;
import io.indexr.util.UTF8Util;
import kafka.consumer.ConsumerIterator;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class NifiFetcher implements Fetcher {
    private static final Logger logger = LoggerFactory.getLogger(NifiFetcher.class);

    private SiteToSiteClient siteToSiteClient;

    private Transaction transaction;

    private final UTF8JsonRowCreator utf8JsonRowCreator;

    private Iterator<byte[]> eventItr;

    private volatile List<byte[]> remaining;

    private boolean hasData;

    private boolean hasNextShouldReturnFalse;

    private SegmentSchema tableSchema;

    private List<byte[]> dataList = new ArrayList<>();


    @JsonProperty("number.empty.as.zero")
    public boolean numberEmptyAsZero;

    @JsonProperty("properties")
    public final Properties properties;

    @JsonCreator
    public NifiFetcher(@JsonProperty("number.empty.as.zero") Boolean numberEmptyAsZero,
                       @JsonProperty("properties") Properties properties) {

        this.properties = properties;

        logger.info("properties {}",properties);
        String urlString = properties.getProperty("nifi.connection.url");
        String portName = properties.getProperty("nifi.connection.portName");
        String requestBatchCount = properties.getProperty("nifi.connection.requestBatchCount");
        SiteToSiteClient.Builder builder = new SiteToSiteClient.Builder()
                .url(urlString)
                .portName(portName)
                .requestBatchCount(Integer.valueOf(requestBatchCount));

        this.utf8JsonRowCreator = new UTF8JsonRowCreator(this.numberEmptyAsZero);
        siteToSiteClient = builder.build();
        logger.info("siteToSiteClient {}", siteToSiteClient);
        hasData = false;
        hasNextShouldReturnFalse = false;
    }

    @Override
    public void setRowCreator(String name, UTF8Row.Creator rowCreator) {
        utf8JsonRowCreator.setRowCreator(name, rowCreator);
    }

    @Override
    public boolean ensure(SegmentSchema schema) throws Exception {
        logger.info("ensure");
        this.tableSchema = schema;
        return true;
    }

    @Override
    public boolean hasNext() throws Exception {
        if (hasNextShouldReturnFalse) {
            return false;
        }
        transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
        DataPacket dataPacket = transaction.receive();
        if (dataPacket == null) {
            return false;
        }
        transaction.cancel("hasNext");
        logger.info("hasNext returning true");
        return true;
    }

    @Override
    public List<UTF8Row> next() throws Exception {

        transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
        DataPacket dataPacket;
        List<UTF8Row> utf8RowsToReturn = new ArrayList<>();
        while (true) {
            dataPacket = transaction.receive();
            if (dataPacket == null) {
                hasNextShouldReturnFalse = true;
                break;
            }

            final InputStream in = dataPacket.getData();
            final long size = dataPacket.getSize();
            final byte[] buff = new byte[(int) size];

            StreamUtils.fillBuffer(in, buff);
            logger.info("buff {}", new String(buff));
            List<UTF8Row> utf8Rows = parseUTF8Row(buff);
            utf8RowsToReturn.addAll(utf8Rows);
            logger.info("utf8Rows {}", utf8Rows.size());
        }
        logger.info("utf8RowsToReturn size {}", utf8RowsToReturn.size());
        return utf8RowsToReturn;
    }

    private List<UTF8Row> parseUTF8Row(byte[] data) {
        try {
            return utf8JsonRowCreator.create(data);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Illegal data: {} ", UTF8Util.fromUtf8(data), e);
            }
            return Collections.emptyList();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        logger.info("nifi close");
        siteToSiteClient.close();
    }

    @Override
    public void commit() {
        logger.info("nifi commit");
        try {
            transaction.confirm();
            transaction.complete();
            hasNextShouldReturnFalse = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        String settings = JsonUtil.toJson(this);
        return String.format("Kafka 0.8 fetcher: %s", settings);
    }

    @Override
    public boolean equals(Fetcher o) {
        return super.equals(o);
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

    private Iterator<byte[]> toItr(ConsumerIterator<byte[], byte[]> consumerIterator) {
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return consumerIterator.hasNext();
            }

            @Override
            public byte[] next() {
                return consumerIterator.next().message();
            }
        };
    }
}
