package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.util.Collections;

public class JsonSchema<T> implements Schema<T> {
    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo("JSON", null, SchemaType.JSON, Collections.emptyMap());
    protected final ObjectMapper json;
    protected final JavaType type;
    protected final SchemaInfo schemaInfo;

    public JsonSchema(ObjectMapper json, Type type) {
        this(json, json.constructType(type));
    }

    public JsonSchema(ObjectMapper json, TypeReference<T> type) {
        this(json, json.constructType(type));
    }

    public JsonSchema(ObjectMapper json, JavaType type) {
        this(json, type, SCHEMA_INFO);
    }

    public JsonSchema(ObjectMapper json, JavaType type, SchemaInfo schemaInfo) {
        this.json = json;
        this.type = type;
        this.schemaInfo = schemaInfo;
    }

    @Override
    public byte[] encode(T message) {
        try {
            return json.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            return json.readValue(bytes, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public Schema<T> clone() {
        return new JsonSchema<>(json, type);
    }
}
