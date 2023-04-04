package com.microsoft.azure.kusto.kafka.connect.sink.it;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ITSetup {
    private static final Logger log = LoggerFactory.getLogger(ITSetup.class);

    protected static void createConnectorJar() throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        Files.createDirectories(Paths.get("target/kafka-sink-azure-kusto"));
        try (OutputStream fos = Files.newOutputStream(
                Paths.get("target/kafka-sink-azure-kusto/kafka-sink-azure-kusto.jar"));
                JarOutputStream target = new JarOutputStream(fos, manifest)) {
            add(new File("target/classes"), target);
        }
        String url = "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.3.2/kafka-connect-avro-converter-7.3.2.jar";
        String fileName = url.substring(url.lastIndexOf('/') + 1);
        log.info("Downloading {} to {}", url, fileName);
        try (InputStream in = new URL(url).openStream()) {
            Files.copy(in, Paths.get("target/kafka-sink-azure-kusto/" + fileName), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Downloading {} to {} failed ", url, fileName, e);
            throw new RuntimeException(e);
        }
    }

    private static void add(File source, JarOutputStream target) throws IOException {
        String name = source.getPath().replace("\\", "/").replace("target/classes/", "");
        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (File nestedFile : Objects.requireNonNull(source.listFiles())) {
                add(nestedFile, target);
            }
        } else {
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(source.toPath()))) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1) {
                        break;
                    }
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }

    static ITCoordinates getConnectorProperties() throws Exception {
        String testPrefix = "tmpKafkaSinkIT_";
        String appId = getProperty("appId", "", false);
        String appKey = getProperty("appKey", "", false);
        String authority = getProperty("authority", "", false);
        String cluster = getProperty("cluster", "", false);
        String database = getProperty("database", "e2e", true);
        String defaultTable = testPrefix + UUID.randomUUID().toString().replace('-', '_');
        String table = getProperty("table", defaultTable, true);
        return new ITCoordinates(appId, appKey, authority, cluster, database, table);
    }

    private static String getProperty(String attribute, String defaultValue, boolean sanitize) {
        String value = System.getProperty(attribute);
        if (value == null) {
            value = System.getenv(attribute);
        }
        value = StringUtils.isEmpty(value) ? defaultValue : value;
        return sanitize ? FilenameUtils.normalizeNoEndSeparator(value) : value;
    }

}
