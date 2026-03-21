package group.gnometrading.backtest.bridge;

import group.gnometrading.schemas.MBP10Decoder;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.MBP1Decoder;
import group.gnometrading.schemas.MBP1Schema;
import group.gnometrading.schemas.MessageHeaderDecoder;
import group.gnometrading.schemas.Schema;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Reconstructs a Java Schema from raw SBE-encoded bytes, typically produced by the Python gnomepy encoder.
 * Reads the template ID from the SBE message header to determine the schema type, then copies the bytes
 * into the schema's internal buffer and re-wraps the decoder so field reads return the encoded values.
 */
public final class BytesSchemaFactory {

    private BytesSchemaFactory() {}

    public static Schema fromBytes(byte[] data) {
        UnsafeBuffer temp = new UnsafeBuffer(data);
        MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(temp, 0);
        int templateId = header.templateId();

        switch (templateId) {
            case MBP10Decoder.TEMPLATE_ID -> {
                MBP10Schema schema = new MBP10Schema();
                schema.buffer.putBytes(0, data, 0, data.length);
                schema.decoder.wrapAndApplyHeader(schema.buffer, 0, schema.messageHeaderDecoder);
                return schema;
            }
            case MBP1Decoder.TEMPLATE_ID -> {
                MBP1Schema schema = new MBP1Schema();
                schema.buffer.putBytes(0, data, 0, data.length);
                schema.decoder.wrapAndApplyHeader(schema.buffer, 0, schema.messageHeaderDecoder);
                return schema;
            }
            default -> throw new IllegalArgumentException("Unsupported SBE template ID: " + templateId);
        }
    }
}
