package com.microsoft.azure.kusto.kafka.connect.sink;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.microsoft.azure.kusto.kafka.connect.sink.it.ITSetup.BOOTSTRAP_ADDRESS;

public class Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private Utils() {

    }

    @Contract(pure = true)
    public static @NotNull String getConnectPath() {
        return "/kafka/connect/kafka-sink-azure-kusto";
    }

    public static @NotNull Map<String, String> getConnectProperties() {
        Map<String, String> env = new HashMap<>();
        env.put("BOOTSTRAP_SERVERS", BOOTSTRAP_ADDRESS);
        env.put("CONNECT_BOOTSTRAP_SERVERS", BOOTSTRAP_ADDRESS);
        env.put("CONNECT_GROUP_ID", "kusto-e2e-connect-group");
        env.put("CONNECT_CONFIG_STORAGE_TOPIC", "connect-config");
        env.put("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets");
        env.put("CONNECT_STATUS_STORAGE_TOPIC", "connect-status");
        env.put("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO");
        env.put("CONNECT_LOG4J_LOGGERS", "org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR");
        env.put("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        env.put("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        env.put("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        env.put("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        env.put("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        env.put("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        env.put("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        env.put("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter");
        env.put("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter");
        env.put("CONNECT_REST_ADVERTISED_HOST_NAME", "kusto-e2e-connect");
        env.put("CONNECT_REST_PORT", String.valueOf(8083));
        env.put("CONNECT_PLUGIN_PATH", Utils.getConnectPath());
        env.put("CLASSPATH", Utils.getConnectPath());
        return env;
    }

    public static @NotNull File getCurrentWorkingDirectory() {
        File currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                Utils.class.getSimpleName(),
                String.valueOf(Instant.now().toEpochMilli())).toString());
        boolean opResult = restrictPermissions(currentDirectory);
        String fullPath = currentDirectory.getAbsolutePath();
        if (!opResult) {
            LOGGER.warn("Setting permissions on the file {} failed", fullPath);
        }
        currentDirectory.deleteOnExit();
        return currentDirectory;
    }

    public static boolean createDirectoryWithPermissions(String path) {
        File folder = new File(FilenameUtils.normalize(path));
        folder.deleteOnExit();
        boolean opResult = restrictPermissions(folder);
        if (!opResult) {
            LOGGER.warn("Setting creating folder {} with permissions", path);
        }
        return folder.mkdirs();
    }

    public static boolean restrictPermissions(File file) {
        // No execute permissions. Read and write only for the owning applications
        try {
            return file.setExecutable(false, false) &&
                    file.setReadable(true, true) &&
                    file.setWritable(true, true);
        } catch (Exception ex) {
            LOGGER.debug("Exception setting permissions on temporary test files[{}]. This is usually not a problem as it is" +
                    "run on test.To fix this, please check if there are specific security policies on test host that are" +
                    "causing this", file.getPath(), ex);
            return false;
        }
    }

    public static int getFilesCount(String path) {
        File folder = new File(path);
        return Objects.requireNonNull(folder.list(), String.format("File %s is empty and has no files", path)).length;
    }
}
