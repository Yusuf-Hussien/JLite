package jlite.executor;

import jlite.catalogue.Catalogue;
import jlite.catalogue.Column;
import jlite.catalogue.DataType;
import jlite.catalogue.TableSchema;
import jlite.storage.BufferPool;
import jlite.storage.Page;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class PersistentTableStore implements TableStore {

    private static final String FILE_MAGIC = "JLITE_TABLE_SLOT_V1";
    private static final String PAGED_FILE_MAGIC = "JLITE_TABLE_PAGED_V1";
    private static final String LEGACY_FILE_MAGIC = "JLITE_TABLE_V1";

    private static final int METADATA_LENGTH_PREFIX_BYTES = Integer.BYTES;
    private static final int PAGE_HEADER_SIZE = Integer.BYTES;
    private static final int PAYLOAD_BYTES_PER_PAGE = Page.PAGE_SIZE - PAGE_HEADER_SIZE;

    private static final int SLOT_PAGE_TUPLE_COUNT_OFFSET = 0;
    private static final int SLOT_PAGE_FREE_START_OFFSET = Integer.BYTES;
    private static final int SLOT_PAGE_HEADER_SIZE = Integer.BYTES * 2;
    private static final int SLOT_ENTRY_SIZE = Integer.BYTES * 2;

    private final InMemoryTableStore delegate;
    private final Path storageDir;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public PersistentTableStore(Catalogue catalogue, Path storageDir) {
        this.delegate = new InMemoryTableStore(catalogue);
        this.storageDir = storageDir;
        init();
        load();
    }

    @Override
    public void createTable(TableSchema schema) {
        writeLocked(() -> {
            delegate.createTable(schema);
            persistTable(schema.name());
            return null;
        });
    }

    @Override
    public void dropTable(String tableName) {
        writeLocked(() -> {
            delegate.dropTable(tableName);
            deleteTableFile(tableName);
            return null;
        });
    }

    @Override
    public void addColumn(String tableName, Column column) {
        writeLocked(() -> {
            delegate.addColumn(tableName, column);
            persistTable(tableName);
            return null;
        });
    }

    @Override
    public void dropColumn(String tableName, String columnName) {
        writeLocked(() -> {
            delegate.dropColumn(tableName, columnName);
            persistTable(tableName);
            return null;
        });
    }

    @Override
    public void insertRow(String tableName, Map<String, Object> values) {
        writeLocked(() -> {
            delegate.insertRow(tableName, values);

            var tableFile = tableFile(tableName);
            if (!isSlottedTableFile(tableFile)) {
                persistTable(tableName);
                return null;
            }

            appendRowToSlottedTable(tableName, values);
            return null;
        });
    }

    @Override
    public int updateRows(String tableName, Predicate<Map<String, Object>> predicate, UnaryOperator<Map<String, Object>> updater) {
        return writeLocked(() -> {
            var affected = delegate.updateRows(tableName, predicate, updater);
            if (affected > 0) {
                var tableFile = tableFile(tableName);
                if (!tryUpdateRowsInPlace(tableName, tableFile, predicate, updater, affected)) {
                    persistTable(tableName);
                }
            }
            return affected;
        });
    }

    @Override
    public int deleteRows(String tableName, Predicate<Map<String, Object>> predicate) {
        return writeLocked(() -> {
            var affected = delegate.deleteRows(tableName, predicate);
            if (affected > 0) {
                var tableFile = tableFile(tableName);
                if (!tryDeleteRowsInPlace(tableName, tableFile, predicate, affected)) {
                    persistTable(tableName);
                }
            }
            return affected;
        });
    }

    @Override
    public List<Map<String, Object>> scan(String tableName) {
        return readLocked(() -> delegate.scan(tableName));
    }

    @Override
    public TableSchema resolveSchema(String tableName) {
        return readLocked(() -> delegate.resolveSchema(tableName));
    }

    @Override
    public List<Column> columns(String tableName) {
        return readLocked(() -> delegate.columns(tableName));
    }

    private void init() {
        try {
            Files.createDirectories(storageDir);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to create storage directory: " + storageDir, ex);
        }
    }

    private void load() {
        try (Stream<Path> stream = Files.list(storageDir)) {
            stream
                .filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(".tbl"))
                .sorted()
                .forEach(this::loadTableFile);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to load table files from: " + storageDir, ex);
        }
    }

    private void loadTableFile(Path file) {
        if (tryLoadSlottedTableFile(file)) {
            return;
        }

        loadLegacyTableFile(file);
    }

    private boolean tryLoadSlottedTableFile(Path file) {
        try (var access = new RandomAccessFile(file.toFile(), "r")) {
            var metadata = readSlottedMetadata(access, file);
            delegate.createTable(metadata.schema());

            var dataPages = (access.length() - metadata.dataStart()) / Page.PAGE_SIZE;
            for (int pageIndex = 0; pageIndex < dataPages; pageIndex++) {
                var page = new byte[Page.PAGE_SIZE];
                access.seek(metadata.dataStart() + (long) pageIndex * Page.PAGE_SIZE);
                access.readFully(page);

                for (var encodedRow : extractRowsFromSlottedPage(page, file, pageIndex)) {
                    var rowValues = decodeRow(encodedRow, metadata.schema().columns(), file);
                    delegate.insertRow(metadata.schema().name(), rowValues);
                }
            }

            var actualRows = delegate.scan(metadata.schema().name()).size();
            if (actualRows != metadata.rowCount()) {
                throw new IllegalStateException("Corrupt slotted table file (row count mismatch): " + file);
            }
            return true;
        } catch (IllegalStateException ex) {
            return false;
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to read table file: " + file, ex);
        }
    }

    private void loadLegacyTableFile(Path file) {
        var lines = readTableLines(file);

        if (lines.size() < 3 || (!PAGED_FILE_MAGIC.equals(lines.get(0)) && !LEGACY_FILE_MAGIC.equals(lines.get(0)))) {
            throw new IllegalStateException("Invalid table file format: " + file);
        }

        var tableName = tableNameFromFile(file);
        var columnCount = parsePositiveInt(lines.get(1), "column count", file);
        if (lines.size() < 3 + columnCount) {
            throw new IllegalStateException("Corrupt table file (missing columns): " + file);
        }

        var columns = new ArrayList<Column>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columns.add(decodeColumn(lines.get(2 + i), file));
        }

        var rowCountIndex = 2 + columnCount;
        if (lines.size() <= rowCountIndex) {
            throw new IllegalStateException("Corrupt table file (missing row count): " + file);
        }

        var rowCount = parsePositiveInt(lines.get(rowCountIndex), "row count", file);
        var expectedTotal = rowCountIndex + 1 + rowCount;
        if (lines.size() != expectedTotal) {
            throw new IllegalStateException("Corrupt table file (row count mismatch): " + file);
        }

        delegate.createTable(new TableSchema(tableName, List.copyOf(columns)));
        for (int i = 0; i < rowCount; i++) {
            var rowLine = lines.get(rowCountIndex + 1 + i);
            var rowValues = decodeRow(rowLine, columns, file);
            delegate.insertRow(tableName, rowValues);
        }
    }

    private void persistTable(String tableName) {
        var schema = delegate.resolveSchema(tableName);
        var rows = delegate.scan(tableName);

        var metadataPayload = encodeSlottedMetadata(schema, rows.size());
        var dataPages = buildSlottedDataPages(schema, rows, tableName);
        var dataStart = alignToPage(METADATA_LENGTH_PREFIX_BYTES + metadataPayload.length);

        var fileBytes = new byte[dataStart + (dataPages.size() * Page.PAGE_SIZE)];
        writeInt(fileBytes, 0, metadataPayload.length);
        System.arraycopy(metadataPayload, 0, fileBytes, METADATA_LENGTH_PREFIX_BYTES, metadataPayload.length);

        for (int pageIndex = 0; pageIndex < dataPages.size(); pageIndex++) {
            var pageOffset = dataStart + (pageIndex * Page.PAGE_SIZE);
            var page = dataPages.get(pageIndex);
            System.arraycopy(page, 0, fileBytes, pageOffset, Page.PAGE_SIZE);
        }

        var target = tableFile(tableName);
        var temp = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            Files.write(temp, fileBytes);
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to persist table: " + tableName, ex);
        }
    }

    private void appendRowToSlottedTable(String tableName, Map<String, Object> values) {
        var tableFile = tableFile(tableName);
        try (var access = new RandomAccessFile(tableFile.toFile(), "rw")) {
            var metadata = readSlottedMetadata(access, tableFile);
            var encodedRow = encodeRow(metadata.schema(), values).getBytes(StandardCharsets.UTF_8);

            var dataPages = (access.length() - metadata.dataStart()) / Page.PAGE_SIZE;
            var inserted = false;
            for (int pageIndex = 0; pageIndex < dataPages; pageIndex++) {
                var pageOffset = metadata.dataStart() + ((long) pageIndex * Page.PAGE_SIZE);
                var page = new byte[Page.PAGE_SIZE];
                access.seek(pageOffset);
                access.readFully(page);

                if (insertIntoSlottedPage(page, encodedRow)) {
                    access.seek(pageOffset);
                    access.write(page);
                    inserted = true;
                    break;
                }
            }

            if (!inserted) {
                var newPage = newEmptySlottedPage();
                if (!insertIntoSlottedPage(newPage, encodedRow)) {
                    throw new IllegalStateException("Row is too large to fit in a single page for table: " + tableName);
                }
                access.seek(access.length());
                access.write(newPage);
            }

            var updatedMetadata = encodeSlottedMetadata(metadata.schema(), metadata.rowCount() + 1);
            if (updatedMetadata.length != metadata.metadataLength()) {
                // Schema changes should go through full persist path and keep this length stable.
                persistTable(tableName);
                return;
            }

            access.seek(0);
            access.writeInt(updatedMetadata.length);
            access.write(updatedMetadata);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to append row to table: " + tableName, ex);
        }
    }

    private boolean tryUpdateRowsInPlace(
        String tableName,
        Path tableFile,
        Predicate<Map<String, Object>> predicate,
        UnaryOperator<Map<String, Object>> updater,
        int expectedAffected
    ) {
        if (!isSlottedTableFile(tableFile)) {
            return false;
        }

        try (var access = new RandomAccessFile(tableFile.toFile(), "rw")) {
            var metadata = readSlottedMetadata(access, tableFile);
            if (!metadata.schema().name().equalsIgnoreCase(tableName)) {
                return false;
            }

            var affected = 0;
            var dataPages = (access.length() - metadata.dataStart()) / Page.PAGE_SIZE;
            for (int pageIndex = 0; pageIndex < dataPages; pageIndex++) {
                var pageOffset = metadata.dataStart() + ((long) pageIndex * Page.PAGE_SIZE);
                var page = new byte[Page.PAGE_SIZE];
                access.seek(pageOffset);
                access.readFully(page);

                var tupleCount = readInt(page, SLOT_PAGE_TUPLE_COUNT_OFFSET);
                var pageDirty = false;
                for (int slot = 0; slot < tupleCount; slot++) {
                    var slotOffset = SLOT_PAGE_HEADER_SIZE + (slot * SLOT_ENTRY_SIZE);
                    var rowOffset = readInt(page, slotOffset);
                    var rowLength = readInt(page, slotOffset + Integer.BYTES);
                    if (rowLength <= 0) {
                        continue;
                    }

                    if (rowOffset < 0 || rowOffset + rowLength > Page.PAGE_SIZE) {
                        throw new IllegalStateException("Corrupt tuple payload in " + tableFile + " at page " + pageIndex);
                    }

                    var encodedRow = new String(page, rowOffset, rowLength, StandardCharsets.UTF_8);
                    var rowValues = decodeRow(encodedRow, metadata.schema().columns(), tableFile);
                    if (!predicate.test(rowValues)) {
                        continue;
                    }

                    affected++;
                    var updated = updater.apply(new LinkedHashMap<>(rowValues));
                    var updatedEncoded = encodeRow(metadata.schema(), updated).getBytes(StandardCharsets.UTF_8);
                    if (updatedEncoded.length > rowLength) {
                        return false;
                    }

                    System.arraycopy(updatedEncoded, 0, page, rowOffset, updatedEncoded.length);
                    writeInt(page, slotOffset + Integer.BYTES, updatedEncoded.length);
                    pageDirty = true;
                }

                if (pageDirty) {
                    access.seek(pageOffset);
                    access.write(page);
                }
            }

            return affected == expectedAffected;
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to update rows in place for table: " + tableName, ex);
        }
    }

    private boolean tryDeleteRowsInPlace(
        String tableName,
        Path tableFile,
        Predicate<Map<String, Object>> predicate,
        int expectedAffected
    ) {
        if (!isSlottedTableFile(tableFile)) {
            return false;
        }

        try (var access = new RandomAccessFile(tableFile.toFile(), "rw")) {
            var metadata = readSlottedMetadata(access, tableFile);
            if (!metadata.schema().name().equalsIgnoreCase(tableName)) {
                return false;
            }

            var affected = 0;
            var dataPages = (access.length() - metadata.dataStart()) / Page.PAGE_SIZE;
            for (int pageIndex = 0; pageIndex < dataPages; pageIndex++) {
                var pageOffset = metadata.dataStart() + ((long) pageIndex * Page.PAGE_SIZE);
                var page = new byte[Page.PAGE_SIZE];
                access.seek(pageOffset);
                access.readFully(page);

                var tupleCount = readInt(page, SLOT_PAGE_TUPLE_COUNT_OFFSET);
                var pageDirty = false;
                for (int slot = 0; slot < tupleCount; slot++) {
                    var slotOffset = SLOT_PAGE_HEADER_SIZE + (slot * SLOT_ENTRY_SIZE);
                    var rowOffset = readInt(page, slotOffset);
                    var rowLength = readInt(page, slotOffset + Integer.BYTES);
                    if (rowLength <= 0) {
                        continue;
                    }

                    if (rowOffset < 0 || rowOffset + rowLength > Page.PAGE_SIZE) {
                        throw new IllegalStateException("Corrupt tuple payload in " + tableFile + " at page " + pageIndex);
                    }

                    var encodedRow = new String(page, rowOffset, rowLength, StandardCharsets.UTF_8);
                    var rowValues = decodeRow(encodedRow, metadata.schema().columns(), tableFile);
                    if (!predicate.test(rowValues)) {
                        continue;
                    }

                    writeInt(page, slotOffset + Integer.BYTES, 0);
                    pageDirty = true;
                    affected++;
                }

                if (pageDirty) {
                    access.seek(pageOffset);
                    access.write(page);
                }
            }

            if (affected != expectedAffected) {
                return false;
            }

            var updatedMetadata = encodeSlottedMetadata(metadata.schema(), metadata.rowCount() - affected);
            if (updatedMetadata.length != metadata.metadataLength()) {
                return false;
            }

            access.seek(0);
            access.writeInt(updatedMetadata.length);
            access.write(updatedMetadata);
            return true;
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to delete rows in place for table: " + tableName, ex);
        }
    }

    private boolean isSlottedTableFile(Path file) {
        if (!Files.isRegularFile(file)) {
            return false;
        }

        try (var access = new RandomAccessFile(file.toFile(), "r")) {
            readSlottedMetadata(access, file);
            return true;
        } catch (IOException | IllegalStateException ex) {
            return false;
        }
    }

    private List<byte[]> buildSlottedDataPages(TableSchema schema, List<Map<String, Object>> rows, String tableName) {
        var pages = new ArrayList<byte[]>();
        var current = newEmptySlottedPage();

        for (var row : rows) {
            var encodedRow = encodeRow(schema, row).getBytes(StandardCharsets.UTF_8);
            if (!insertIntoSlottedPage(current, encodedRow)) {
                pages.add(current);
                current = newEmptySlottedPage();
                if (!insertIntoSlottedPage(current, encodedRow)) {
                    throw new IllegalStateException("Row is too large to fit in a single page for table: " + tableName);
                }
            }
        }

        if (readInt(current, SLOT_PAGE_TUPLE_COUNT_OFFSET) > 0) {
            pages.add(current);
        }

        return pages;
    }

    private byte[] newEmptySlottedPage() {
        var page = new byte[Page.PAGE_SIZE];
        writeInt(page, SLOT_PAGE_TUPLE_COUNT_OFFSET, 0);
        writeInt(page, SLOT_PAGE_FREE_START_OFFSET, Page.PAGE_SIZE);
        return page;
    }

    private boolean insertIntoSlottedPage(byte[] page, byte[] rowPayload) {
        var tupleCount = readInt(page, SLOT_PAGE_TUPLE_COUNT_OFFSET);
        var freeStart = readInt(page, SLOT_PAGE_FREE_START_OFFSET);
        if (tupleCount < 0 || freeStart < SLOT_PAGE_HEADER_SIZE || freeStart > Page.PAGE_SIZE) {
            throw new IllegalStateException("Corrupt slotted page header");
        }

        var slotStart = SLOT_PAGE_HEADER_SIZE + (tupleCount * SLOT_ENTRY_SIZE);
        var availableBytes = freeStart - slotStart;
        if (availableBytes < rowPayload.length + SLOT_ENTRY_SIZE) {
            return false;
        }

        var rowOffset = freeStart - rowPayload.length;
        System.arraycopy(rowPayload, 0, page, rowOffset, rowPayload.length);

        var slotOffset = SLOT_PAGE_HEADER_SIZE + (tupleCount * SLOT_ENTRY_SIZE);
        writeInt(page, slotOffset, rowOffset);
        writeInt(page, slotOffset + Integer.BYTES, rowPayload.length);

        writeInt(page, SLOT_PAGE_TUPLE_COUNT_OFFSET, tupleCount + 1);
        writeInt(page, SLOT_PAGE_FREE_START_OFFSET, rowOffset);
        return true;
    }

    private List<String> extractRowsFromSlottedPage(byte[] page, Path file, int pageIndex) {
        var tupleCount = readInt(page, SLOT_PAGE_TUPLE_COUNT_OFFSET);
        var freeStart = readInt(page, SLOT_PAGE_FREE_START_OFFSET);
        if (tupleCount < 0 || freeStart < SLOT_PAGE_HEADER_SIZE || freeStart > Page.PAGE_SIZE) {
            throw new IllegalStateException("Corrupt slotted page header in " + file + " at page " + pageIndex);
        }

        var rows = new ArrayList<String>(tupleCount);
        for (int slot = 0; slot < tupleCount; slot++) {
            var slotOffset = SLOT_PAGE_HEADER_SIZE + (slot * SLOT_ENTRY_SIZE);
            if (slotOffset + SLOT_ENTRY_SIZE > Page.PAGE_SIZE) {
                throw new IllegalStateException("Corrupt slot table in " + file + " at page " + pageIndex);
            }

            var rowOffset = readInt(page, slotOffset);
            var rowLength = readInt(page, slotOffset + Integer.BYTES);
            if (rowLength == 0) {
                continue;
            }
            if (rowOffset < 0 || rowLength < 0 || rowOffset + rowLength > Page.PAGE_SIZE) {
                throw new IllegalStateException("Corrupt tuple payload in " + file + " at page " + pageIndex);
            }

            rows.add(new String(page, rowOffset, rowLength, StandardCharsets.UTF_8));
        }

        return rows;
    }

    private TableFileMetadata readSlottedMetadata(RandomAccessFile access, Path file) throws IOException {
        if (access.length() < METADATA_LENGTH_PREFIX_BYTES) {
            throw new IllegalStateException("Invalid slotted table file header: " + file);
        }

        access.seek(0);
        var metadataLength = access.readInt();
        if (metadataLength <= 0 || metadataLength > access.length() - METADATA_LENGTH_PREFIX_BYTES) {
            throw new IllegalStateException("Invalid slotted metadata length in " + file);
        }

        var metadataPayload = new byte[metadataLength];
        access.readFully(metadataPayload);
        var decoded = decodeSlottedMetadata(metadataPayload, file);
        var dataStart = alignToPage(METADATA_LENGTH_PREFIX_BYTES + metadataLength);
        if (access.length() < dataStart || (access.length() - dataStart) % Page.PAGE_SIZE != 0) {
            throw new IllegalStateException("Invalid slotted data page alignment in " + file);
        }

        return new TableFileMetadata(decoded.schema(), decoded.rowCount(), metadataLength, dataStart);
    }

    private byte[] encodeSlottedMetadata(TableSchema schema, int rowCount) {
        var output = new ByteArrayOutputStream();
        try (var data = new DataOutputStream(output)) {
            data.writeUTF(FILE_MAGIC);
            data.writeInt(schema.columns().size());
            for (var column : schema.columns()) {
                data.writeUTF(column.name());
                data.writeUTF(column.type().name());
                data.writeBoolean(column.nullable());
                data.writeBoolean(column.primaryKey());
            }
            data.writeInt(rowCount);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to encode slotted metadata", ex);
        }
        return output.toByteArray();
    }

    private DecodedMetadata decodeSlottedMetadata(byte[] payload, Path file) {
        try (var data = new DataInputStream(new ByteArrayInputStream(payload))) {
            var magic = data.readUTF();
            if (!FILE_MAGIC.equals(magic)) {
                throw new IllegalStateException("Invalid slotted magic in " + file);
            }

            var columnCount = data.readInt();
            if (columnCount < 0) {
                throw new IllegalStateException("Invalid slotted column count in " + file + ": " + columnCount);
            }

            var columns = new ArrayList<Column>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(new Column(
                    data.readUTF(),
                    DataType.valueOf(data.readUTF()),
                    data.readBoolean(),
                    data.readBoolean()
                ));
            }

            var rowCount = data.readInt();
            if (rowCount < 0) {
                throw new IllegalStateException("Invalid slotted row count in " + file + ": " + rowCount);
            }
            if (data.available() != 0) {
                throw new IllegalStateException("Trailing slotted metadata bytes in " + file);
            }

            var tableName = tableNameFromFile(file);
            return new DecodedMetadata(new TableSchema(tableName, List.copyOf(columns)), rowCount);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to decode slotted metadata from: " + file, ex);
        }
    }

    private List<String> readTableLines(Path file) {
        try {
            var pagedContent = fromPagedBytes(Files.readAllBytes(file), file);
            return decodeLines(pagedContent);
        } catch (IllegalStateException ex) {
            return readLegacyTextLines(file);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to read table file: " + file, ex);
        }
    }

    private List<String> readLegacyTextLines(Path file) {
        try {
            return Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to read table file: " + file, ex);
        }
    }

    private List<String> decodeLines(byte[] payload) {
        var content = new String(payload, StandardCharsets.UTF_8);
        if (content.isEmpty()) {
            return List.of();
        }
        return List.of(content.split("\\n", -1));
    }

    private byte[] fromPagedBytes(byte[] rawFile, Path file) {
        if (rawFile.length == 0 || rawFile.length % Page.PAGE_SIZE != 0) {
            throw new IllegalStateException("Table file is not page aligned: " + file);
        }

        var pageCount = rawFile.length / Page.PAGE_SIZE;
        var bufferPool = new BufferPool(pageCount);
        var output = new ByteArrayOutputStream(rawFile.length);

        for (int pageId = 0; pageId < pageCount; pageId++) {
            var page = bufferPool.fetchPage(pageId);
            var pageBytes = page.getData();

            System.arraycopy(rawFile, pageId * Page.PAGE_SIZE, pageBytes, 0, Page.PAGE_SIZE);
            var payloadLength = readInt(pageBytes, 0);
            if (payloadLength < 0 || payloadLength > PAYLOAD_BYTES_PER_PAGE) {
                throw new IllegalStateException("Invalid page payload length in " + file + " at page " + pageId);
            }

            if (payloadLength > 0) {
                output.write(pageBytes, PAGE_HEADER_SIZE, payloadLength);
            }
        }

        return output.toByteArray();
    }

    private int readInt(byte[] source, int offset) {
        return ((source[offset] & 0xFF) << 24)
            | ((source[offset + 1] & 0xFF) << 16)
            | ((source[offset + 2] & 0xFF) << 8)
            | (source[offset + 3] & 0xFF);
    }

    private void writeInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private <T> T readLocked(SupplierWithException<T> supplier) {
        var readLock = lock.readLock();
        readLock.lock();
        try {
            return supplier.get();
        } finally {
            readLock.unlock();
        }
    }

    private <T> T writeLocked(SupplierWithException<T> supplier) {
        var writeLock = lock.writeLock();
        writeLock.lock();
        try {
            return supplier.get();
        } finally {
            writeLock.unlock();
        }
    }

    private void deleteTableFile(String tableName) {
        var file = tableFile(tableName);
        try {
            Files.deleteIfExists(file);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to delete table file: " + file, ex);
        }
    }

    private Path tableFile(String tableName) {
        return storageDir.resolve(tableName + ".tbl");
    }

    private String tableNameFromFile(Path file) {
        var name = file.getFileName().toString();
        return name.substring(0, name.length() - 4);
    }

    private int parsePositiveInt(String value, String fieldName, Path file) {
        try {
            var parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalStateException("Invalid " + fieldName + " in " + file + ": " + value);
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Invalid " + fieldName + " in " + file + ": " + value, ex);
        }
    }

    private Column decodeColumn(String encoded, Path file) {
        var parts = encoded.split("\\|", -1);
        if (parts.length != 4) {
            throw new IllegalStateException("Invalid column entry in " + file + ": " + encoded);
        }
        return new Column(
            fromBase64(parts[0]),
            DataType.valueOf(parts[1]),
            Boolean.parseBoolean(parts[2]),
            Boolean.parseBoolean(parts[3])
        );
    }

    private String encodeRow(TableSchema schema, Map<String, Object> row) {
        var encodedValues = new ArrayList<String>(schema.columns().size());
        for (var column : schema.columns()) {
            encodedValues.add(encodeValue(row.get(column.name()), column.type()));
        }
        return String.join("|", encodedValues);
    }

    private Map<String, Object> decodeRow(String encoded, List<Column> columns, Path file) {
        var parts = encoded.split("\\|", -1);
        if (parts.length != columns.size()) {
            throw new IllegalStateException("Invalid row entry in " + file + ": " + encoded);
        }
        var row = new LinkedHashMap<String, Object>();
        for (int i = 0; i < columns.size(); i++) {
            var column = columns.get(i);
            row.put(column.name(), decodeValue(parts[i], column.type()));
        }
        return row;
    }

    private String encodeValue(Object value, DataType type) {
        if (value == null) {
            return "N";
        }

        return switch (type) {
            case INT, BIGINT -> "L:" + ((Number) value).longValue();
            case FLOAT, DOUBLE -> "D:" + ((Number) value).doubleValue();
            case BOOLEAN -> "B:" + value;
            case TEXT, VARCHAR, DATE, TIMESTAMP -> "S:" + base64(String.valueOf(value));
        };
    }

    private Object decodeValue(String encoded, DataType type) {
        if ("N".equals(encoded)) {
            return null;
        }
        if (encoded.length() < 3 || encoded.charAt(1) != ':') {
            throw new IllegalStateException("Invalid encoded value: " + encoded);
        }

        var payload = encoded.substring(2);
        return switch (type) {
            case INT, BIGINT -> Long.valueOf(payload);
            case FLOAT, DOUBLE -> Double.valueOf(payload);
            case BOOLEAN -> "true".equalsIgnoreCase(payload);
            case TEXT, VARCHAR, DATE, TIMESTAMP -> fromBase64(payload);
        };
    }

    private String base64(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private String fromBase64(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private int alignToPage(int value) {
        var remainder = value % Page.PAGE_SIZE;
        return remainder == 0 ? value : value + (Page.PAGE_SIZE - remainder);
    }

    private record DecodedMetadata(TableSchema schema, int rowCount) {
    }

    private record TableFileMetadata(TableSchema schema, int rowCount, int metadataLength, int dataStart) {
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {
        T get();
    }
}