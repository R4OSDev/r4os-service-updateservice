const std = @import("std");
const settings = @import("r4std").settings;

pub const schema = "UPDSVC_DOWNLOAD";
pub const package_id_capacity: usize = 48;
pub const package_version_capacity: usize = 24;
pub const release_capacity: usize = 24;
pub const filename_capacity: usize = 128;
pub const sha256_capacity: usize = 64;
pub const url_capacity: usize = 1024;
pub const journal_capacity: usize = 2048;

pub const State = enum(u16) {
    available = 0,
    downloading = 1,
    downloaded = 2,
    failed = 3,
};

pub const PartialAction = union(enum) {
    start,
    continue_from: u64,
    verify,
    discard,
};

/// Der Downloadsatz ist die unveraenderliche Bindung an genau ein Angebot
/// eines Suchsnapshots. Die PART-Datei allein ist nie ausreichend, um einen
/// Download nach einem Dienstneustart fortzusetzen.
pub const Record = struct {
    job_id: u32 = 0,
    search_job_id: u32 = 0,
    result_index: u32 = 0,
    state: State = .available,
    result: i32 = 0,
    expected_size: u64 = 0,
    progress: u64 = 0,
    package_id_len: u16 = 0,
    package_version_len: u16 = 0,
    release_len: u16 = 0,
    filename_len: u16 = 0,
    sha256_len: u16 = 0,
    url_len: u16 = 0,
    package_id: [package_id_capacity]u8 = .{0} ** package_id_capacity,
    package_version: [package_version_capacity]u8 = .{0} ** package_version_capacity,
    release: [release_capacity]u8 = .{0} ** release_capacity,
    filename: [filename_capacity]u8 = .{0} ** filename_capacity,
    sha256: [sha256_capacity]u8 = .{0} ** sha256_capacity,
    url: [url_capacity]u8 = .{0} ** url_capacity,

    pub fn init(
        job_id: u32,
        search_job_id: u32,
        result_index: u32,
        package_id: []const u8,
        package_version: []const u8,
        release: []const u8,
        filename: []const u8,
        sha256: []const u8,
        url: []const u8,
        expected_size: u64,
    ) ?Record {
        var record = Record{
            .job_id = job_id,
            .search_job_id = search_job_id,
            .result_index = result_index,
            .state = .downloading,
            .expected_size = expected_size,
        };
        if (!copyText(record.package_id[0..], &record.package_id_len, package_id) or
            !copyText(record.package_version[0..], &record.package_version_len, package_version) or
            !copyText(record.release[0..], &record.release_len, release) or
            !copyText(record.filename[0..], &record.filename_len, filename) or
            !copyText(record.sha256[0..], &record.sha256_len, sha256) or
            !copyText(record.url[0..], &record.url_len, url) or !record.valid()) return null;
        return record;
    }

    pub fn valid(self: *const Record) bool {
        return self.job_id != 0 and self.search_job_id != 0 and self.expected_size != 0 and
            self.expected_size <= std.math.maxInt(u32) and self.progress <= self.expected_size and
            self.package_id_len != 0 and self.package_id_len <= self.package_id.len and
            self.package_version_len != 0 and self.package_version_len <= self.package_version.len and
            self.release_len != 0 and self.release_len <= self.release.len and
            self.filename_len != 0 and self.filename_len <= self.filename.len and
            self.sha256_len == self.sha256.len and self.url_len != 0 and self.url_len <= self.url.len and
            validFilename(self.filenameText()) and validSha256(self.sha256Text()) and
            validLineText(self.packageIdText()) and validLineText(self.packageVersionText()) and
            validLineText(self.releaseText()) and validHttpsUrl(self.urlText());
    }

    pub fn packageIdText(self: *const Record) []const u8 {
        return self.package_id[0..@min(@as(usize, self.package_id_len), self.package_id.len)];
    }

    pub fn packageVersionText(self: *const Record) []const u8 {
        return self.package_version[0..@min(@as(usize, self.package_version_len), self.package_version.len)];
    }

    pub fn releaseText(self: *const Record) []const u8 {
        return self.release[0..@min(@as(usize, self.release_len), self.release.len)];
    }

    pub fn filenameText(self: *const Record) []const u8 {
        return self.filename[0..@min(@as(usize, self.filename_len), self.filename.len)];
    }

    pub fn sha256Text(self: *const Record) []const u8 {
        return self.sha256[0..@min(@as(usize, self.sha256_len), self.sha256.len)];
    }

    pub fn urlText(self: *const Record) []const u8 {
        return self.url[0..@min(@as(usize, self.url_len), self.url.len)];
    }

    pub fn sameOffer(self: *const Record, other: *const Record) bool {
        return self.search_job_id == other.search_job_id and self.result_index == other.result_index and
            self.expected_size == other.expected_size and
            std.mem.eql(u8, self.packageIdText(), other.packageIdText()) and
            std.mem.eql(u8, self.packageVersionText(), other.packageVersionText()) and
            std.mem.eql(u8, self.releaseText(), other.releaseText()) and
            std.mem.eql(u8, self.filenameText(), other.filenameText()) and
            std.ascii.eqlIgnoreCase(self.sha256Text(), other.sha256Text()) and
            std.mem.eql(u8, self.urlText(), other.urlText());
    }
};

pub fn encode(record: *const Record, out: []u8) ?[]const u8 {
    if (!record.valid()) return null;
    var writer = settings.Writer.init(out);
    writer.writeHeader(schema);
    writer.writePairU32("JOB_ID", record.job_id);
    writer.writePairU32("SEARCH_JOB_ID", record.search_job_id);
    writer.writePairU32("RESULT_INDEX", record.result_index);
    writer.writePairU32("STATE", @intFromEnum(record.state));
    writer.writePairI32("RESULT", record.result);
    writer.writePairU32("EXPECTED_SIZE", @intCast(record.expected_size));
    writer.writePairU32("PROGRESS", @intCast(record.progress));
    writer.writePair("PACKAGE_ID", record.packageIdText());
    writer.writePair("PACKAGE_VERSION", record.packageVersionText());
    writer.writePair("RELEASE", record.releaseText());
    writer.writePair("FILENAME", record.filenameText());
    writer.writePair("SHA256", record.sha256Text());
    writer.writePair("URL", record.urlText());
    return if (writer.ok()) writer.bytes() else null;
}

pub fn parse(bytes: []const u8, out: *Record) bool {
    const document = settings.Document.init(bytes);
    if (!document.hasSupportedFormat()) return false;
    if (!std.mem.eql(u8, document.schemaName() orelse return false, schema)) return false;
    const raw_state = document.u32Value("STATE") orelse return false;
    const state = switch (raw_state) {
        0 => State.available,
        1 => State.downloading,
        2 => State.downloaded,
        3 => State.failed,
        else => return false,
    };
    var record = Record{
        .job_id = document.u32Value("JOB_ID") orelse return false,
        .search_job_id = document.u32Value("SEARCH_JOB_ID") orelse return false,
        .result_index = document.u32Value("RESULT_INDEX") orelse return false,
        .state = state,
        .result = document.i32Value("RESULT") orelse return false,
        .expected_size = document.u32Value("EXPECTED_SIZE") orelse return false,
        .progress = document.u32Value("PROGRESS") orelse return false,
    };
    if (!copyText(record.package_id[0..], &record.package_id_len, document.value("PACKAGE_ID") orelse return false) or
        !copyText(record.package_version[0..], &record.package_version_len, document.value("PACKAGE_VERSION") orelse return false) or
        !copyText(record.release[0..], &record.release_len, document.value("RELEASE") orelse return false) or
        !copyText(record.filename[0..], &record.filename_len, document.value("FILENAME") orelse return false) or
        !copyText(record.sha256[0..], &record.sha256_len, document.value("SHA256") orelse return false) or
        !copyText(record.url[0..], &record.url_len, document.value("URL") orelse return false) or !record.valid()) return false;
    out.* = record;
    return true;
}

pub fn partialAction(expected_size: u64, observed_size: ?u64) PartialAction {
    const size = observed_size orelse return .start;
    if (size > expected_size) return .discard;
    if (size == expected_size) return .verify;
    if (size == 0) return .start;
    return .{ .continue_from = size };
}

/// Liest exakt die erwartete Dateilaenge und prueft zusaetzlich ein Byte
/// dahinter. Zu kurze und zu lange Dateien koennen dadurch nicht denselben
/// Hashpfad wie ein gueltiges Paket erreichen.
pub fn verifyReader(reader: anytype, path: [*:0]const u8, expected_size: u64, expected_sha256: []const u8, scratch: []u8) bool {
    if (expected_size == 0 or expected_size > std.math.maxInt(u32) or scratch.len == 0 or !validSha256(expected_sha256)) return false;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: u64 = 0;
    while (offset < expected_size) {
        const want: usize = @intCast(@min(@as(u64, scratch.len), expected_size - offset));
        const got = reader.fileReadAt(path, @intCast(offset), scratch[0..want]);
        if (got <= 0 or got > @as(i32, @intCast(want))) return false;
        const count: usize = @intCast(got);
        hasher.update(scratch[0..count]);
        offset += count;
    }
    var extra: [1]u8 = undefined;
    if (reader.fileReadAt(path, @intCast(expected_size), extra[0..]) != 0) return false;
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.ascii.eqlIgnoreCase(hex[0..], expected_sha256);
}

fn copyText(out: []u8, length: *u16, value: []const u8) bool {
    if (value.len > out.len or value.len > std.math.maxInt(u16)) return false;
    @memset(out, 0);
    if (value.len != 0) @memcpy(out[0..value.len], value);
    length.* = @intCast(value.len);
    return true;
}

fn validLineText(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |byte| if (byte < 0x20 or byte == 0x7f or byte == '=') return false;
    return true;
}

fn validFilename(value: []const u8) bool {
    if (value.len < 5 or value.len > filename_capacity or !std.ascii.endsWithIgnoreCase(value, ".R4U")) return false;
    for (value) |byte| {
        if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_' and byte != '.') return false;
    }
    return true;
}

fn validSha256(value: []const u8) bool {
    if (value.len != sha256_capacity) return false;
    for (value) |byte| if (!std.ascii.isHex(byte)) return false;
    return true;
}

fn validHttpsUrl(value: []const u8) bool {
    if (value.len <= "https://".len or value.len > url_capacity or !std.ascii.startsWithIgnoreCase(value, "https://")) return false;
    for (value) |byte| if (byte <= ' ' or byte >= 0x7f or byte == '\\' or byte == '#') return false;
    return true;
}

const MemoryReader = struct {
    bytes: []const u8,

    fn fileReadAt(self: *const MemoryReader, path: [*:0]const u8, offset: u32, out: []u8) i32 {
        _ = path;
        if (offset >= self.bytes.len) return 0;
        const count = @min(out.len, self.bytes.len - offset);
        @memcpy(out[0..count], self.bytes[offset .. offset + count]);
        return @intCast(count);
    }
};

fn fixtureRecord() Record {
    return Record.init(
        7,
        5,
        2,
        "R4SYS-BASE",
        "0.2.0",
        "0.63.18",
        "R4SYS-BASE-0.2.0.R4U",
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "https://r4os.r4server.eu:1339/packages/R4SYS-BASE-0.2.0.R4U",
        3,
    ).?;
}

test "download journal round trips an immutable search snapshot" {
    try @import("r4std_test").ensure();
    const original = fixtureRecord();
    var bytes: [journal_capacity]u8 = undefined;
    const encoded = encode(&original, bytes[0..]) orelse return error.TestUnexpectedResult;
    var parsed = Record{};
    try std.testing.expect(parse(encoded, &parsed));
    try std.testing.expect(original.sameOffer(&parsed));
    try std.testing.expectEqual(State.downloading, parsed.state);

    parsed.url[parsed.url_len - 1] = 'X';
    try std.testing.expect(!original.sameOffer(&parsed));
}

test "partial downloads resume at deterministic byte boundaries" {
    try @import("r4std_test").ensure();
    const expected: u64 = 11 * 1024 * 1024;
    try std.testing.expect(partialAction(expected, null) == .start);
    try std.testing.expect(partialAction(expected, 0) == .start);
    for ([_]u64{ 1, 4093, 16 * 1024, expected - 1 }) |boundary| {
        const action = partialAction(expected, boundary);
        try std.testing.expectEqual(boundary, action.continue_from);
    }
    try std.testing.expect(partialAction(expected, expected) == .verify);
    try std.testing.expect(partialAction(expected, expected + 1) == .discard);
}

test "size and SHA256 bind the final package after resume" {
    try @import("r4std_test").ensure();
    const good = MemoryReader{ .bytes = "abc" };
    var scratch: [2]u8 = undefined;
    try std.testing.expect(verifyReader(&good, "X", 3, fixtureRecord().sha256Text(), scratch[0..]));

    const corrupt = MemoryReader{ .bytes = "abd" };
    const short = MemoryReader{ .bytes = "ab" };
    const long = MemoryReader{ .bytes = "abcd" };
    try std.testing.expect(!verifyReader(&corrupt, "X", 3, fixtureRecord().sha256Text(), scratch[0..]));
    try std.testing.expect(!verifyReader(&short, "X", 3, fixtureRecord().sha256Text(), scratch[0..]));
    try std.testing.expect(!verifyReader(&long, "X", 3, fixtureRecord().sha256Text(), scratch[0..]));
}

test "fixture restart resumes an 11 MB package byte exactly at multiple boundaries" {
    try @import("r4std_test").ensure();
    const allocator = std.testing.allocator;
    const size: usize = 11 * 1024 * 1024;
    const source = try allocator.alloc(u8, size);
    defer allocator.free(source);
    for (source, 0..) |*byte, index| byte.* = @truncate((index * 131 + 17) ^ (index >> 5));
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(source, &digest, .{});
    const digest_hex = std.fmt.bytesToHex(digest, .lower);

    for ([_]usize{ 0, 1, 4093, 16 * 1024, size - 1 }) |boundary| {
        const part = try allocator.alloc(u8, size);
        defer allocator.free(part);
        @memcpy(part[0..boundary], source[0..boundary]);
        var before = Record.init(
            19,
            17,
            0,
            "FIXTURE",
            "1.0.0",
            "0.63.18",
            "FIXTURE-1.0.0.R4U",
            digest_hex[0..],
            "https://r4os.r4server.eu:1339/packages/FIXTURE-1.0.0.R4U",
            size,
        ) orelse return error.TestUnexpectedResult;
        before.progress = boundary;
        var journal: [journal_capacity]u8 = undefined;
        const persisted = encode(&before, journal[0..]) orelse return error.TestUnexpectedResult;

        var after_restart = Record{};
        try std.testing.expect(parse(persisted, &after_restart));
        const action = partialAction(after_restart.expected_size, boundary);
        const resume_from: usize = switch (action) {
            .start => 0,
            .continue_from => |offset| @intCast(offset),
            else => return error.TestUnexpectedResult,
        };
        @memcpy(part[resume_from..], source[resume_from..]);
        const reader = MemoryReader{ .bytes = part };
        var scratch: [4093]u8 = undefined;
        try std.testing.expect(verifyReader(&reader, "X", size, after_restart.sha256Text(), scratch[0..]));
        try std.testing.expectEqualSlices(u8, source, part);
    }
}
