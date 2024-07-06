package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueOptions;
import io.nats.client.Message;
import io.nats.client.api.*;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.Validator.*;

public class CljNatsKeyValue extends NatsKeyValue implements KeyValue {
    CljNatsKeyValue(NatsConnection connection, String bucketName, KeyValueOptions kvo) throws IOException {
        super(connection, bucketName, kvo);
    }

    public MessageInfo getMessage(String key) throws IOException, JetStreamApiException {
        return _getLast(readSubject(validateNonWildcardKvKeyRequired(key)));
    }

    public MessageInfo getMessage(String key, long revision) throws IOException, JetStreamApiException {
        return _getMessage(validateNonWildcardKvKeyRequired(key), revision);
    }

    MessageInfo _getMessage(String key, long revision) throws IOException, JetStreamApiException {
        MessageInfo mi = _getBySeq(revision);
        if (mi != null) {
            KeyValueEntry kve = new KeyValueEntry(mi);
            if (key.equals(kve.getKey())) {
                return mi;
            }
        }
        return null;
    }

    public long put(String key, byte[] value, Headers h) throws IOException, JetStreamApiException {
        return _write(key, value, h).getSeqno();
    }

    private PublishAck _write(String key, byte[] data, Headers h) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        return js.publish(NatsMessage.builder().subject(writeSubject(key)).data(data).headers(h).build());
    }

    public List<Message> getHistory(String key) throws IOException, JetStreamApiException, InterruptedException {
        validateNonWildcardKvKeyRequired(key);
        List<Message> list = new ArrayList<>();
        visitSubject(readSubject(key), DeliverPolicy.All, false, true, m -> list.add(m));
        return list;
    }

    public static CljNatsKeyValue create(Connection connection, String bucketName, KeyValueOptions options) throws IOException {
        NatsConnection conn = (NatsConnection) connection;
        Validator.validateBucketName(bucketName, true);

        if (conn.isClosing() || conn.isClosed()) {
            throw new IOException("A JetStream context can't be established during close.");
        }

        return new CljNatsKeyValue(conn, bucketName, options);
    }
}
