package org.apache.nifi.processors.redis;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({ "redis", "deduplication" })
@CapabilityDescription("A NiFi processor that deduplicates incoming data using an existing Redis connection pool service.")
public class RedisDeduplicator extends AbstractProcessor {

	public static final PropertyDescriptor REDIS_SERVICE = new PropertyDescriptor.Builder().name("redisConnectionPool")
			.description("Specifies the Redis connection pool service to use for connections to Redis.").required(true)
			.identifiesControllerService(RedisConnectionPool.class).build();

	public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder().name("ttl").description(
			"Indicates how long the data should exist in Redis. Setting '0 secs' would mean the data would exist forever")
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).required(true).defaultValue("0 secs").build();

	public static final PropertyDescriptor SOURCE_TYPE = new PropertyDescriptor.Builder().name("sourceType")
			.description("Indicates type of incoming data - deafault is json").required(true).allowableValues("json")
			.defaultValue("json").build();

	public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder().name("key").description(
			"Indicates json paths (comma separated list) whose value will be used as key in Redis. Eg- for input {\"person\":{\"name\":\"Rahul\",\"email\":\"rahul@xyz.com\"}}, <jsonPaths> = $.person.name,$.person.email and <key in Redis> = Rahul-rahul@xyz.com")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor VALUE = new PropertyDescriptor.Builder().name("value").description(
			"Indicates json path whose value will be used as value in Redis. It can be null as well, empty string will be stored in this case. Eg- for input {\"person\":{\"name\":\"Rahul\",\"email\":\"rahul@xyz.com\"}}, <jsonPath> = $.person.email and <value in Redis> = rahul@xyz.com")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor ROUNDING_INTERVAL = new PropertyDescriptor.Builder().name("roundingInterval")
			.description(
					"The interval in seconds used for rounding timestamps before storing data in Redis. Use '0 secs' for no rounding.")
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).required(true).defaultValue("0 secs").build();

	public static final PropertyDescriptor TIMESTAMP_FIELD_TO_ROUND = new PropertyDescriptor.Builder()
			.name("timestampFieldToRound")
			.description(
					"The json path of the field containing timestamps (millis) to be rounded to the nearest interval.")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor ATTRIBUTE_ADDED_FOR_LOG = new PropertyDescriptor.Builder()
			.name("attributeAddedForLog").description("Specifies the attribute added for log purposes.")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship DUPLICATES_NOT_FOUND = new Relationship.Builder().name("DUPLICATES_NOT_FOUND")
			.description("No duplicate entry found in Redis storage").build();

	public static final Relationship DUPLICATES_FOUND = new Relationship.Builder().name("DUPLICATES_FOUND")
			.description("Duplicate entry found in Redis storage").build();

	public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
			.description("Failed to process the flowfile").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	private volatile RedisConnectionPool redisConnectionPool;
	private RedisConnection redisConnection;
	private Long ttl;
	private String sourceType;
	private String[] keyJsonPaths;
	private String valueJsonPath;
	private Long roundingInterval;
	private String timestampFieldToRoundJsonPath;
	private String attributeAddedForLog;
	private boolean duplicatesFound;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(REDIS_SERVICE);
		descriptors.add(TTL);
		descriptors.add(SOURCE_TYPE);
		descriptors.add(KEY);
		descriptors.add(VALUE);
		descriptors.add(ROUNDING_INTERVAL);
		descriptors.add(TIMESTAMP_FIELD_TO_ROUND);
		descriptors.add(ATTRIBUTE_ADDED_FOR_LOG);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(DUPLICATES_NOT_FOUND);
		relationships.add(DUPLICATES_FOUND);
		relationships.add(FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		// connection
		redisConnectionPool = context.getProperty(REDIS_SERVICE).asControllerService(RedisConnectionPool.class);
		redisConnection = redisConnectionPool.getConnection();

		// TTL
		ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS);
		if (ttl == 0) {
			ttl = -1L;
		}

		// other properties
		sourceType = context.getProperty(SOURCE_TYPE).getValue();
		keyJsonPaths = context.getProperty(KEY).getValue().split(",");
		valueJsonPath = context.getProperty(VALUE).getValue();
		attributeAddedForLog = context.getProperty(ATTRIBUTE_ADDED_FOR_LOG).getValue();

		// rounding
		roundingInterval = context.getProperty(ROUNDING_INTERVAL).asTimePeriod(TimeUnit.SECONDS) * 1000;
		timestampFieldToRoundJsonPath = context.getProperty(TIMESTAMP_FIELD_TO_ROUND).getValue();
		if (roundingInterval > 0
				&& (timestampFieldToRoundJsonPath == null || "".equals(timestampFieldToRoundJsonPath))) {
			throw new IllegalStateException(
					"timestampFieldToRound can not be null/empty when ROUNDING_INTERVAL is greater than 0 millis.");
		} else {
			boolean found = false;
			for (String keyPath : keyJsonPaths) {
				if (timestampFieldToRoundJsonPath.equals(keyPath)) {
					found = true;
					break;
				}
			}
			if (!found) {
				throw new IllegalArgumentException("timestampFieldToRound must be one of the key json paths.");
			}
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		// sourceType check
		if (!sourceType.equals("json")) {
			getLogger().error("Failed to process {} data, sourceType {} not supported; transferring to failure",
					new Object[] { flowFile, sourceType });
			if (attributeAddedForLog != null || !"".equals(attributeAddedForLog)) {
				flowFile = session.putAttribute(flowFile, attributeAddedForLog,
						"sourceType - " + sourceType + " not supported");
			}
			session.transfer(flowFile, FAILURE);
			return;
		}

		// process the flowFile
		duplicatesFound = true;
		try {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(InputStream rawIn) throws IOException {
					try (final InputStream in = new BufferedInputStream(rawIn)) {
						// parse incoming data
						String json = IOUtils.toString(in, "UTF-8");
						Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);

						// extract redis key
						String key = "";
						for (int i = 0; i < keyJsonPaths.length; i++) {
							Object result = JsonPath.read(document, keyJsonPaths[i]);
							if (roundingInterval > 0 && keyJsonPaths[i].equals(timestampFieldToRoundJsonPath)) {
								Long timestamp = (result instanceof Integer) ? (long) ((Integer) result)
										: (Long) result;
								timestamp = (timestamp / roundingInterval) * roundingInterval;
								key = (i == 0) ? timestamp.toString() : key + "-" + timestamp.toString();
							} else {
								key = (i == 0) ? result.toString() : key + "-" + result.toString();
							}
						}

						// extract redis value
						String value = (valueJsonPath == null || "".equals(valueJsonPath)) ? ""
								: JsonPath.read(document, valueJsonPath).toString();

						// convert string to bytes to be using in redis
						byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
						byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

						// check duplicates
						duplicatesFound = redisConnection.exists(keyBytes);

						// add is no entry found
						if (!duplicatesFound) {
							redisConnection.set(keyBytes, valueBytes, Expiration.seconds(ttl),
									RedisStringCommands.SetOption.upsert());
						}
					}
				}
			});
		} catch (final ProcessException pe) {
			getLogger().error("Failed to process {} data due to {}; transferring to failure",
					new Object[] { flowFile, pe });
			if (attributeAddedForLog != null || !"".equals(attributeAddedForLog)) {
				flowFile = session.putAttribute(flowFile, attributeAddedForLog, "Failed to process flowfile - " + pe);
			}
			session.transfer(flowFile, FAILURE);
			return;
		}

		// return
		if (duplicatesFound) {
			if (attributeAddedForLog != null || !"".equals(attributeAddedForLog)) {
				flowFile = session.putAttribute(flowFile, attributeAddedForLog, DUPLICATES_FOUND.getDescription());
			}
			session.transfer(flowFile, DUPLICATES_FOUND);
		} else {
			if (attributeAddedForLog != null || !"".equals(attributeAddedForLog)) {
				flowFile = session.putAttribute(flowFile, attributeAddedForLog, DUPLICATES_NOT_FOUND.getDescription());
			}
			session.transfer(flowFile, DUPLICATES_NOT_FOUND);
		}
	}	
}
