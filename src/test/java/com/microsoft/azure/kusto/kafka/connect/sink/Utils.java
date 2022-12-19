package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(FileWriter.class);

    private Utils() {

    }

    public static File getCurrentWorkingDirectory() {
        File currentDirectory = new File(Paths.get(
                System.getProperty("java.io.tmpdir"),
                FileWriter.class.getSimpleName(),
                String.valueOf(Instant.now().toEpochMilli())).toString());
        boolean opResult = restrictPermissions(currentDirectory);
        String fullPath = currentDirectory.getAbsolutePath();
        if (!opResult) {
            log.warn("Setting permissions on the file {} failed", fullPath);
        }
        currentDirectory.deleteOnExit();
        return currentDirectory;
    }

    public static boolean createDirectoryWithPermissions(String path) {
        File folder = new File(FilenameUtils.normalize(path));
        folder.deleteOnExit();
        boolean opResult = restrictPermissions(folder);
        if (!opResult) {
            log.warn("Setting creating folder {} with permissions", path);
        }
        return folder.mkdirs();
    }

    public static boolean restrictPermissions(File file) {
        // No execute permissions. Read and write only for the owning applications
        return file.setExecutable(false, false) &&
                file.setReadable(true, true) &&
                file.setWritable(true, true);
    }

    public static int getFilesCount(String path) {
        File folder = new File(path);
        return Objects.requireNonNull(folder.list(), String.format("File %s is empty and has no files", path)).length;
    }
}
