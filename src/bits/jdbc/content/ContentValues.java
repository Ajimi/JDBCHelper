package bits.jdbc.content;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public final class ContentValues implements Serializable {
    public static final String TAG = "ContentValues";
    /**
     * Holds the actual values
     */
    private HashMap<String, Object> mValues;

    /**
     * Creates an empty set of values using the default initial size
     */
    public ContentValues() {
        // Choosing a default size of 8 based on analysis of typical
        // consumption by applications.
        mValues = new HashMap<String, Object>(8);
    }

    /**
     * Creates an empty set of values using the given initial size
     *
     * @param size the initial size of the set of values
     */
    public ContentValues(int size) {
        mValues = new HashMap<String, Object>(size, 1.0f);
    }

    /**
     * Creates a set of values copied from the given set
     *
     * @param from the values to copy
     */
    public ContentValues(ContentValues from) {
        mValues = new HashMap<String, Object>(from.mValues);
    }

    /**
     * Creates a set of values copied from the given HashMap. This is used
     * by the Parcel unmarshalling code.
     *
     * @param values the values to start with
     *               {@hide}
     */
    private ContentValues(HashMap<String, Object> values) {
        mValues = values;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof ContentValues)) {
            return false;
        }
        return mValues.equals(((ContentValues) object).mValues);
    }

    @Override
    public int hashCode() {
        return mValues.hashCode();
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, String value) {
        mValues.put(key, value);
    }

    /**
     * Adds all values from the passed in ContentValues.
     *
     * @param other the ContentValues from which to copy
     */
    public void putAll(ContentValues other) {
        mValues.putAll(other.mValues);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, Byte value) {
        mValues.put(key, value);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, Short value) {
        mValues.put(key, value);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, Integer value) {
        mValues.put(key, value);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, Long value) {
        mValues.put(key, value);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, Float value) {
        mValues.put(key, value);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, Double value) {
        mValues.put(key, value);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, Boolean value) {
        mValues.put(key, value);
    }

    /**
     * Adds a value to the set.
     *
     * @param key   the name of the value to put
     * @param value the data for the value to put
     */
    public void put(String key, byte[] value) {
        mValues.put(key, value);
    }

    /**
     * Adds a null value to the set.
     *
     * @param key the name of the value to make null
     */
    public void putNull(String key) {
        mValues.put(key, null);
    }

    /**
     * Returns the number of values.
     *
     * @return the number of values
     */
    public int size() {
        return mValues.size();
    }

    /**
     * Indicates whether this collection is empty.
     *
     * @return true iff size == 0
     * {@hide}
     * TODO: consider exposing this new method publicly
     */
    public boolean isEmpty() {
        return mValues.isEmpty();
    }

    /**
     * Remove a single value.
     *
     * @param key the name of the value to remove
     */
    public void remove(String key) {
        mValues.remove(key);
    }

    /**
     * Removes all values.
     */
    public void clear() {
        mValues.clear();
    }

    /**
     * Returns true if this object has the named value.
     *
     * @param key the value to check for
     * @return {@code true} if the value is present, {@code false} otherwise
     */
    public boolean containsKey(String key) {
        return mValues.containsKey(key);
    }

    /**
     * Gets a value. Valid value types are {@link String}, {@link Boolean},
     * {@link Number}, and {@code byte[]} implementations.
     *
     * @param key the value to get
     * @return the data for the value, or {@code null} if the value is missing or if {@code null}
     * was previously added with the given {@code key}
     */
    public Object get(String key) {
        return mValues.get(key);
    }

    /**
     * Gets a value and converts it to a String.
     *
     * @param key the value to get
     * @return the String for the value
     */
    public String getAsString(String key) {
        Object value = mValues.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Gets a value and converts it to a Long.
     *
     * @param key the value to get
     * @return the Long value, or {@code null} if the value is missing or cannot be converted
     */
    public Long getAsLong(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).longValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Long.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    System.out.println("Cannot parse Long value for " + value + " at key " + key);
                    return null;
                }
            } else {
                System.out.println("Cannot cast value for " + key + " to a Long: " + value);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to an Integer.
     *
     * @param key the value to get
     * @return the Integer value, or {@code null} if the value is missing or cannot be converted
     */
    public Integer getAsInteger(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).intValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Integer.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    System.out.println("Cannot parse Integer value for " + value + " at key " + key);
                    return null;
                }
            } else {
                System.out.println("Cannot cast value for " + key + " to a Integer: " + value);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Short.
     *
     * @param key the value to get
     * @return the Short value, or {@code null} if the value is missing or cannot be converted
     */
    public Short getAsShort(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).shortValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Short.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    System.out.println("Cannot parse Short value for " + value + " at key " + key);
                    return null;
                }
            } else {
                System.out.println("Cannot cast value for " + key + " to a Short: " + value);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Byte.
     *
     * @param key the value to get
     * @return the Byte value, or {@code null} if the value is missing or cannot be converted
     */
    public Byte getAsByte(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).byteValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Byte.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    System.out.println("Cannot parse Byte value for " + value + " at key " + key);
                    return null;
                }
            } else {
                System.out.println("Cannot cast value for " + key + " to a Byte: " + value);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Double.
     *
     * @param key the value to get
     * @return the Double value, or {@code null} if the value is missing or cannot be converted
     */
    public Double getAsDouble(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).doubleValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Double.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    System.out.println("Cannot parse Double value for " + value + " at key " + key);
                    return null;
                }
            } else {
                System.out.println("Cannot cast value for " + key + " to a Double: " + value);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Float.
     *
     * @param key the value to get
     * @return the Float value, or {@code null} if the value is missing or cannot be converted
     */
    public Float getAsFloat(String key) {
        Object value = mValues.get(key);
        try {
            return value != null ? ((Number) value).floatValue() : null;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                try {
                    return Float.valueOf(value.toString());
                } catch (NumberFormatException e2) {
                    System.out.println("Cannot parse Float value for " + value + " at key " + key);
                    return null;
                }
            } else {
                System.out.println("Cannot cast value for " + key + " to a Float: " + value);
                return null;
            }
        }
    }

    /**
     * Gets a value and converts it to a Boolean.
     *
     * @param key the value to get
     * @return the Boolean value, or {@code null} if the value is missing or cannot be converted
     */
    public Boolean getAsBoolean(String key) {
        Object value = mValues.get(key);
        try {
            return (Boolean) value;
        } catch (ClassCastException e) {
            if (value instanceof CharSequence) {
                // Note that we also check against 1 here because SQLite's internal representation
                // for booleans is an integer with a value of 0 or 1. Without this check, boolean
                // values obtained via DatabaseUtils#cursorRowToContentValues will always return
                // false.
                return Boolean.valueOf(value.toString()) || "1".equals(value);
            } else if (value instanceof Number) {
                return ((Number) value).intValue() != 0;
            } else {
                System.out.println("Cannot cast value for " + key + " to a Boolean: " + value);
                return null;
            }
        }
    }

    /**
     * Gets a value that is a byte array. Note that this method will not convert
     * any other types to byte arrays.
     *
     * @param key the value to get
     * @return the {@code byte[]} value, or {@code null} is the value is missing or not a
     * {@code byte[]}
     */
    public byte[] getAsByteArray(String key) {
        Object value = mValues.get(key);
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            return null;
        }
    }

    /**
     * Returns a set of all of the keys and values
     *
     * @return a set of all of the keys and values
     */
    public Set<Map.Entry<String, Object>> valueSet() {
        return mValues.entrySet();
    }

    /**
     * Returns a set of all of the keys
     *
     * @return a set of all of the keys
     */
    public Set<String> keySet() {
        return mValues.keySet();
    }

    /**
     * Unsupported, here until we get proper bulk insert APIs.
     * {@hide}
     */
    @Deprecated
    public void putStringArrayList(String key, ArrayList<String> value) {
        mValues.put(key, value);
    }

    /**
     * Unsupported, here until we get proper bulk insert APIs.
     * {@hide}
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public ArrayList<String> getStringArrayList(String key) {
        return (ArrayList<String>) mValues.get(key);
    }

    /**
     * Returns a string containing a concise, human-readable description of this object.
     *
     * @return a printable representation of this object.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String name : mValues.keySet()) {
            String value = getAsString(name);
            if (sb.length() > 0) sb.append(" ");
            sb.append(name).append("=").append(value);
        }
        return sb.toString();
    }
}