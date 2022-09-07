package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class JsonRecordWriterProvider implements RecordWriterProvider {
    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);

    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonConverter converter = new JsonConverter();

    public JsonRecordWriterProvider() {
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "false");
        converterConfig.put("schemas.cache.size", "50");
        this.converter.configure(converterConfig, false);
    }

    @Override
    public RecordWriter getRecordWriter(final String filename, OutputStream out) {
        try {
            log.debug("Opening record writer for: {}", filename);
            return new RecordWriter() {
                final JsonGenerator writer = mapper.getFactory()
                        .createGenerator(out)
                        .setRootValueSeparator(null);

                @Override
                public void write(SinkRecord record) {
                    log.trace("Sink record: {}", record);
                    try {
                        Object value = record.value();
                        if (value instanceof Struct) {
                            byte[] rawJson = converter.fromConnectData(
                                    record.topic(),
                                    record.valueSchema(),
                                    value);
                            out.write(rawJson);
                            out.write(LINE_SEPARATOR_BYTES);
                        } else {
                            writer.writeObject(value);
                            writer.writeRaw(LINE_SEPARATOR);
                        }
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }

                @Override
                public void commit() {
                    try {
                        writer.flush();
                    } catch (IOException e) {
                        throw new DataException(e);
                    }
                }

                @Override
                public void close() {
                    try {
                        writer.close();
                        out.close();
                    } catch (IOException e) {
                        throw new DataException(e);
                    }
                }
            };
        } catch (IOException e) {
            throw new DataException(e);
        }
    }
}
