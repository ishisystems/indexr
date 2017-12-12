package io.indexr.server.rt.fetcher;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.Transaction.TransactionState;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.exception.UnknownPortException;
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
	private final SiteToSiteClient.Builder invalidSiteToSiteClientBuilder;
	private final UTF8JsonRowCreator utf8JsonRowCreator;

	private SiteToSiteClient siteToSiteClient;
	private SiteToSiteClient invalidSiteToSiteClient;
	private Transaction transaction;
	private boolean transactionDone;
	private boolean emptyTransaction;

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
		String invalidPortName = properties.getProperty("nifi.connection.invalidPortName");
		this.siteToSiteClientBuilder = new SiteToSiteClient.Builder();
		this.siteToSiteClientBuilder.url(urlString);
		this.siteToSiteClientBuilder.portName(portName);
		this.siteToSiteClientBuilder.requestBatchCount(Integer.valueOf(requestBatchCount));
		if (invalidPortName != null) {
			this.invalidSiteToSiteClientBuilder = new SiteToSiteClient.Builder();
			invalidSiteToSiteClientBuilder.fromConfig(this.siteToSiteClientBuilder.buildConfig());
			invalidSiteToSiteClientBuilder.portName(invalidPortName);
		} else {
			this.invalidSiteToSiteClientBuilder = null;
		}
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
		if (invalidSiteToSiteClient == null && invalidSiteToSiteClientBuilder != null) {
			invalidSiteToSiteClient = invalidSiteToSiteClientBuilder.build();
		}
		return transaction == null ? createTransaction() : true;
	}

	@Override
	public synchronized boolean hasNext() {
		return transaction != null && !transactionDone;
	}

	@Override
	public List<UTF8Row> next() throws InterruptedException {
		byte[] data = null;
		Map<String, String> attributes = null;
		while (data == null) { // skip all empty transaction until we get some with data
			synchronized (this) {
				if (transaction == null) {
					return Collections.emptyList();
				}
				DataPacket packet = null;
				try {
					packet = transaction.receive();
					if (packet != null) {
						emptyTransaction = false;
						data = IOUtils.toByteArray(packet.getData(), packet.getSize()); // we cannot close the stream
						attributes = packet.getAttributes();
					} else if (emptyTransaction) {
						transaction.confirm();
						transaction.complete();
						transaction = null;
						createTransaction();
						Thread.sleep(100);
					} else {
						transactionDone = true; // transaction done, hasNext() will return false
						return Collections.emptyList();
					}
				} catch (IOException | TransmissionDisabledException e) {
					cancelTransaction("Cannot receive packet from transaction", e);
					return Collections.emptyList();
				}
			}
		}
		// finally we have some data to process
		try {
			synchronized (utf8JsonRowCreator) {
				long failed = utf8JsonRowCreator.getFailCount();
				List<UTF8Row> rows = utf8JsonRowCreator.create(data);
				if (utf8JsonRowCreator.getFailCount() <= failed) {
					return rows;
				}
			}
			// invalid data were already reported inside utf8JsonRowCreator
		} catch (Exception e) {
			LOGGER.debug("Illegal data", e); // some parsing exception, maybe wrong charset?
		}
		try {
			reportInvalidData(data, attributes);
		} catch (Exception e) {
			cancelTransaction("Cannot report invalid data back to NiFi", e);
		}
		return Collections.emptyList();
	}

	private void reportInvalidData(final byte[] data, final Map<String, String> attributes) throws IOException {
		if (this.invalidSiteToSiteClient == null) {
			return;
		}
		Transaction sendTransaction = this.invalidSiteToSiteClient.createTransaction(TransferDirection.SEND);
		if (sendTransaction == null) {
			cancelTransaction("Cannot report invalid data back to NiFi as sending transaction is null", null);
			return;
		}
		sendTransaction.send(data, attributes);
		sendTransaction.confirm();
		sendTransaction.complete();
	}

	private boolean createTransaction() {
		if (siteToSiteClient == null) {
			return false;
		}
		try {
			transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
			transactionDone = false;
			emptyTransaction = true;
			return transaction != null;
		} catch (ConnectException | PortNotRunningException | UnknownPortException e) {
			LOGGER.error("Cannot create transaction: {}", e.toString());
			return false;
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
	public synchronized void commit() throws IOException {
		// we can commit only full transaction, when not done, it will be committed
		// during next iteration
		if (transaction == null || !transactionDone) {
			return;
		}
		try {
			transaction.confirm();
			transaction.complete();
			transaction = null;
		} catch (Exception e) {
			cancelTransaction("Cannot commit transaction", e);
			throw e;
		}
	}

	@Override
	public synchronized void close() throws IOException {
		cancelTransaction("Closing the Fetcher", null);
		IOUtils.closeQuietly(this.siteToSiteClient);
		this.siteToSiteClient = null;
		this.invalidSiteToSiteClient = null;
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
