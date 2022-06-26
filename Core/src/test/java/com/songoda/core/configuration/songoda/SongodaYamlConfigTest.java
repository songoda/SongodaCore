package com.songoda.core.configuration.songoda;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class SongodaYamlConfigTest {
    Path tmpDir;
    Path cfg;

    @BeforeEach
    void setUp() throws IOException {
        this.tmpDir = Files.createTempDirectory("SongodaYamlConfigTest");

        this.cfg = Files.createTempFile(this.tmpDir, "config", ".yml");
        this.tmpDir.toFile().deleteOnExit();
    }

    @AfterEach
    void tearDown() throws IOException {
        try (Stream<Path> stream = Files.walk(this.tmpDir)) {
            stream
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile).forEach(File::delete);
        }
    }

    @Test
    void testLoad() throws IOException {
        Files.write(this.cfg, "test-key: foo\n".getBytes());

        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile());
        cfg.set("test-key", "bar");
        cfg.load();

        assertEquals("foo", cfg.get("test-key"));
    }

    @Test
    void testSave() throws IOException {
        Files.write(this.cfg, "test-key: foo\n".getBytes());

        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile());
        cfg.set("test-key", "bar");
        cfg.save();

        assertEquals("test-key: bar\n", new String(Files.readAllBytes(this.cfg)));
    }

    @Test
    void testWithVersion() throws IOException {
        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile());
        cfg.withVersion("version-key", 1, null);

        assertEquals(1, cfg.get("version-key"));

        cfg.save();
        assertEquals("version-key: 1\n", new String(Files.readAllBytes(this.cfg)));

        cfg.withVersion(2);

        assertEquals(2, cfg.get("version"));
        assertNull(cfg.get("version-key"));

        cfg.save();
        assertEquals(
                "# Don't touch this – it's used to track the version of the config.\n" +
                        "version: 2\n",
                new String(Files.readAllBytes(this.cfg))
        );
    }

    @Test
    void testWithNegativeVersion() {
        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile());
        Assertions.assertThrows(IllegalArgumentException.class, () -> cfg.withVersion("version-key", -1, null));
    }

    @Test
    void testWithTooNewVersion() {
        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile())
                .withVersion(1);

        Assertions.assertThrows(IllegalStateException.class, () -> cfg.load(new StringReader("version: 10\n")));
    }

    @Test
    void testWithUpToDateVersion() throws IOException {
        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile())
                .withVersion(2);

        assertFalse(cfg.upgradeOldConfigVersion());
    }

    @Test
    void testWithKeyWithoutConfigEntry() throws IOException {
        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile());

        cfg.set("test-key", "foo");
        cfg.load();

        assertNull(cfg.get("test-key"));

        cfg.set("test-key", "foo");
        assertEquals("foo", cfg.get("test-key"));

        cfg.save();
        cfg.load();

        assertEquals("foo", cfg.get("test-key"));
        assertEquals(1, cfg.getKeys("").size());
    }

    @Test
    void testDefaultValueAppliedAfterLoadNullValue() {
        SongodaYamlConfig cfg = new SongodaYamlConfig(this.cfg.toFile());
        ConfigEntry entry = new ConfigEntry(cfg, "key", "value");

        cfg.init();

        assertEquals("value", entry.get());
    }
}