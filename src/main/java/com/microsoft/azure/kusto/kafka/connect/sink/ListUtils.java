package com.microsoft.azure.kusto.kafka.connect.sink;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility class for list operations.
 */
public class ListUtils {

    private ListUtils() {
        // Utility class, prevent instantiation
    }

    /**
     * Returns the first element of a list.
     *
     * @param <T> the type of elements in the list
     * @param list the list
     * @return the first element
     * @throws NoSuchElementException if the list is null or empty
     */
    public static <T> T getFirst(List<T> list) {
        if (list == null || list.isEmpty()) {
            throw new NoSuchElementException("List is empty");
        }
        return list.get(0);
    }
}
