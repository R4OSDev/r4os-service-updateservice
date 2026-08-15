const std = @import("std");
const contract = @import("update_service_contract");

pub const SubmitResult = union(enum) {
    accepted: contract.Ack,
    busy: contract.Ack,
    invalid: contract.Ack,
};

pub const CancelResult = union(enum) {
    accepted: contract.Ack,
    missing: contract.Ack,
    busy: contract.Ack,
};

pub const Work = struct {
    job_id: u32,
    operation: contract.Operation,
    request: contract.CommandRequest,
    source_job_id: u32 = 0,
};

pub const Completion = struct {
    state: contract.State,
    result: i32,
    flags: u32 = 0,
    progress_current: u64 = 0,
    progress_total: u64 = 0,
    package_count: u32 = 0,
    completed_count: u32 = 0,
    reason: []const u8 = "",
};

pub const Coordinator = struct {
    lock: u32 = 0,
    status: contract.Status = .{},
    queued: bool = false,
    durable: bool = false,
    active: bool = false,
    next_job_id: u32 = 0,
    work: Work = .{ .job_id = 0, .operation = .search, .request = .{} },
    cancel_requested: u32 = 0,

    pub fn init(config_valid: bool) Coordinator {
        var self = Coordinator{};
        self.status.state = @intFromEnum(contract.State.idle);
        self.status.flags = contract.flag_worker_ready |
            (if (config_valid) contract.flag_config_valid else 0);
        contract.setReason(&self.status, if (config_valid) "ready" else "config-invalid");
        return self;
    }

    pub fn submit(self: *Coordinator, operation_raw: u16, request: contract.CommandRequest) SubmitResult {
        const operation = contract.operationFromWire(operation_raw) orelse
            return .{ .invalid = makeAck(0, operation_raw, contract.State.failed, contract.result_invalid, 0) };
        if (!request.header.valid(@sizeOf(contract.CommandRequest)) or request.selectionText() == null)
            return .{ .invalid = makeAck(0, operation_raw, contract.State.failed, contract.result_invalid, 0) };
        return self.submitPrepared(operation, request, 0, 0);
    }

    pub fn submitDownload(self: *Coordinator, request: contract.DownloadRequest) SubmitResult {
        if (!request.header.valid(@sizeOf(contract.DownloadRequest)) or request.search_job_id == 0)
            return .{ .invalid = makeAck(0, contract.op_download, contract.State.failed, contract.result_invalid, 0) };
        return self.submitPrepared(.download, .{}, request.search_job_id, request.result_index);
    }

    pub fn submitSnapshot(self: *Coordinator, operation: contract.Operation, request: contract.SnapshotRequest) SubmitResult {
        if ((operation != .update_all and operation != .restart) or
            !request.header.valid(@sizeOf(contract.SnapshotRequest)) or request.search_job_id == 0)
            return .{ .invalid = makeAck(0, @intFromEnum(operation), contract.State.failed, contract.result_invalid, 0) };
        return self.submitPrepared(operation, .{}, request.search_job_id, 0);
    }

    fn submitPrepared(
        self: *Coordinator,
        operation: contract.Operation,
        request: contract.CommandRequest,
        source_job_id: u32,
        result_index: u32,
    ) SubmitResult {
        const operation_raw: u16 = @intFromEnum(operation);
        if (!self.tryAcquire())
            return .{ .busy = self.busyAck(operation_raw) };
        defer self.release();

        if (self.queued or self.active or (self.status.flags & contract.flag_busy) != 0)
            return .{ .busy = self.busyAckLocked(operation_raw) };

        self.next_job_id +%= 1;
        if (self.next_job_id == 0) self.next_job_id = 1;
        self.work = .{ .job_id = self.next_job_id, .operation = operation, .request = request, .source_job_id = source_job_id };
        self.queued = true;
        self.durable = false;
        @atomicStore(u32, &self.cancel_requested, 0, .release);
        self.status.job_id = self.next_job_id;
        self.status.generation +%= 1;
        self.status.operation = operation_raw;
        self.status.state = @intFromEnum(contract.State.queued);
        self.status.source_job_id = source_job_id;
        self.status.result_index = result_index;
        self.status.flags = retainedFlags(self.status.flags) | contract.flag_busy | contract.flag_cancelable;
        self.status.result = contract.result_ok;
        self.status.progress_current = 0;
        self.status.progress_total = 0;
        self.status.package_count = 0;
        self.status.completed_count = 0;
        contract.setReason(&self.status, "queued-not-durable");
        return .{ .accepted = self.ackLocked() };
    }

    pub fn markDurable(self: *Coordinator, job_id: u32, persisted: bool) bool {
        if (!self.tryAcquire()) return false;
        defer self.release();
        if (!self.queued or self.work.job_id != job_id) return false;
        if (!persisted) {
            self.queued = false;
            self.status.state = @intFromEnum(contract.State.failed);
            self.status.flags = retainedFlags(self.status.flags);
            self.status.result = contract.result_persist_failed;
            self.status.generation +%= 1;
            contract.setReason(&self.status, "persist-failed");
            return false;
        }
        self.durable = true;
        self.status.generation +%= 1;
        contract.setReason(&self.status, "queued");
        return true;
    }

    pub fn takeWork(self: *Coordinator) ?Work {
        if (!self.tryAcquire()) return null;
        defer self.release();
        if (!self.queued or !self.durable or self.active) return null;
        self.queued = false;
        self.durable = false;
        self.active = true;
        self.status.state = @intFromEnum(initialState(self.work.operation));
        self.status.generation +%= 1;
        contract.setReason(&self.status, contract.operationName(self.status.operation));
        return self.work;
    }

    pub fn complete(self: *Coordinator, job_id: u32, completion: Completion) bool {
        if (!self.tryAcquire()) return false;
        defer self.release();
        if (!self.active or self.work.job_id != job_id) return false;
        self.active = false;
        self.status.state = @intFromEnum(completion.state);
        self.status.result = completion.result;
        self.status.flags = retainedFlags(self.status.flags) | completion.flags;
        self.status.progress_current = completion.progress_current;
        self.status.progress_total = completion.progress_total;
        self.status.package_count = completion.package_count;
        self.status.completed_count = completion.completed_count;
        self.status.generation +%= 1;
        contract.setReason(&self.status, completion.reason);
        @atomicStore(u32, &self.cancel_requested, 0, .release);
        return true;
    }

    pub fn transition(self: *Coordinator, job_id: u32, state: contract.State, reason: []const u8) bool {
        if (!self.tryAcquire()) return false;
        defer self.release();
        if (!self.active or self.work.job_id != job_id) return false;
        self.status.state = @intFromEnum(state);
        self.status.generation +%= 1;
        contract.setReason(&self.status, reason);
        return true;
    }

    pub fn transitionPackage(
        self: *Coordinator,
        job_id: u32,
        state: contract.State,
        source_job_id: u32,
        result_index: u32,
        package_count: u32,
        completed_count: u32,
        reason: []const u8,
    ) bool {
        if (!self.tryAcquire()) return false;
        defer self.release();
        if (!self.active or self.work.job_id != job_id or completed_count > package_count) return false;
        self.status.state = @intFromEnum(state);
        self.status.source_job_id = source_job_id;
        self.status.result_index = result_index;
        self.status.package_count = package_count;
        self.status.completed_count = completed_count;
        self.status.progress_current = 0;
        self.status.progress_total = 0;
        self.status.generation +%= 1;
        contract.setReason(&self.status, reason);
        return true;
    }

    pub fn progress(self: *Coordinator, job_id: u32, current: u64, total: u64) bool {
        if (current > total or !self.tryAcquire()) return false;
        defer self.release();
        if (!self.active or self.work.job_id != job_id) return false;
        self.status.state = @intFromEnum(contract.State.downloading);
        self.status.progress_current = current;
        self.status.progress_total = total;
        self.status.generation +%= 1;
        contract.setReason(&self.status, "downloading");
        return true;
    }

    pub fn cancel(self: *Coordinator, job_id: u32) CancelResult {
        if (!self.tryAcquire())
            return .{ .busy = makeAck(job_id, contract.op_cancel, .cancelling, contract.result_busy, 0) };
        defer self.release();
        if ((!self.queued and !self.active) or (job_id != 0 and job_id != self.status.job_id))
            return .{ .missing = makeAck(job_id, contract.op_cancel, .failed, contract.result_not_found, self.status.generation) };
        @atomicStore(u32, &self.cancel_requested, 1, .release);
        self.status.state = @intFromEnum(contract.State.cancelling);
        self.status.generation +%= 1;
        contract.setReason(&self.status, "cancel-requested");
        return .{ .accepted = self.ackLocked() };
    }

    pub fn cancelRequested(self: *const Coordinator) bool {
        return @atomicLoad(u32, &self.cancel_requested, .acquire) != 0;
    }

    pub fn snapshot(self: *Coordinator) ?contract.Status {
        if (!self.tryAcquire()) return null;
        defer self.release();
        return self.status;
    }

    pub fn restore(self: *Coordinator, saved: contract.Status, config_valid: bool) bool {
        if (!saved.valid() or !self.tryAcquire()) return false;
        defer self.release();
        self.status = saved;
        self.next_job_id = saved.job_id;
        self.queued = false;
        self.durable = false;
        self.active = false;
        @atomicStore(u32, &self.cancel_requested, 0, .release);
        self.status.flags = contract.flag_worker_ready | contract.flag_recovered |
            (if (config_valid) contract.flag_config_valid else 0);
        self.status.generation +%= 1;
        if (stateWasBusy(saved.state)) {
            self.status.state = @intFromEnum(contract.State.interrupted);
            self.status.result = contract.result_not_ready;
            contract.setReason(&self.status, "service-restarted");
        }
        return true;
    }

    /// Reconstructs only an interrupted, already durable download. The
    /// immutable package metadata remains outside the coordinator in the
    /// persisted download record.
    pub fn recoverDownload(self: *Coordinator, job_id: u32) bool {
        if (!self.tryAcquire()) return false;
        defer self.release();
        if (job_id == 0 or self.status.job_id != job_id or self.status.operation != contract.op_download or
            self.status.state != @intFromEnum(contract.State.interrupted) or self.queued or self.active) return false;
        self.work = .{ .job_id = job_id, .operation = .download, .request = .{} };
        self.queued = true;
        self.durable = true;
        self.status.state = @intFromEnum(contract.State.queued);
        self.status.flags = retainedFlags(self.status.flags) | contract.flag_busy | contract.flag_cancelable;
        self.status.result = contract.result_ok;
        self.status.generation +%= 1;
        contract.setReason(&self.status, "download-resume-queued");
        return true;
    }

    pub fn reconcileDownloadCompletion(
        self: *Coordinator,
        job_id: u32,
        state: contract.State,
        result: i32,
        current: u64,
        total: u64,
    ) bool {
        if ((state != .downloaded and state != .failed) or current > total or !self.tryAcquire()) return false;
        defer self.release();
        if (job_id == 0 or self.status.job_id != job_id or self.status.operation != contract.op_download or
            self.status.state != @intFromEnum(contract.State.interrupted) or self.queued or self.active) return false;
        self.status.state = @intFromEnum(state);
        self.status.result = result;
        self.status.progress_current = current;
        self.status.progress_total = total;
        self.status.package_count = 1;
        self.status.completed_count = if (state == .downloaded) 1 else 0;
        self.status.flags = retainedFlags(self.status.flags);
        self.status.generation +%= 1;
        contract.setReason(&self.status, if (state == .downloaded) "downloaded-recovered" else "download-failed-recovered");
        return true;
    }

    pub fn stop(self: *Coordinator) void {
        if (!self.tryAcquire()) return;
        defer self.release();
        self.status.state = @intFromEnum(contract.State.stopping);
        self.status.generation +%= 1;
        contract.setReason(&self.status, "stopping");
        @atomicStore(u32, &self.cancel_requested, 1, .release);
    }

    fn tryAcquire(self: *Coordinator) bool {
        return @cmpxchgStrong(u32, &self.lock, 0, 1, .acquire, .monotonic) == null;
    }

    fn release(self: *Coordinator) void {
        @atomicStore(u32, &self.lock, 0, .release);
    }

    fn busyAck(self: *Coordinator, operation: u16) contract.Ack {
        _ = self;
        // The lock owner may currently update the non-atomic status snapshot.
        // A contended caller therefore gets no speculative job identity.
        return makeAck(0, operation, .queued, contract.result_busy, 0);
    }

    fn busyAckLocked(self: *const Coordinator, operation: u16) contract.Ack {
        return makeAck(self.status.job_id, operation, .queued, contract.result_busy, self.status.generation);
    }

    fn ackLocked(self: *const Coordinator) contract.Ack {
        return .{
            .job_id = self.status.job_id,
            .state = self.status.state,
            .operation = self.status.operation,
            .result = self.status.result,
            .generation = self.status.generation,
        };
    }
};

fn makeAck(job_id: u32, operation: u16, state: contract.State, result: i32, generation: u32) contract.Ack {
    return .{
        .job_id = job_id,
        .state = @intFromEnum(state),
        .operation = operation,
        .result = result,
        .generation = generation,
    };
}

fn retainedFlags(flags: u32) u32 {
    return flags & (contract.flag_config_valid | contract.flag_worker_ready | contract.flag_recovered);
}

fn initialState(operation: contract.Operation) contract.State {
    return switch (operation) {
        .search => .authenticating,
        .download => .downloading,
        .install, .update_all => .verifying,
        .restart => .installing,
    };
}

fn stateWasBusy(raw: u16) bool {
    const state = contract.stateFromWire(raw) orelse return true;
    return switch (state) {
        .queued,
        .authenticating,
        .searching,
        .downloading,
        .verifying,
        .installing,
        .cancelling,
        .stopping,
        => true,
        else => false,
    };
}

test "one durable job owns all clients and a second submit is busy" {
    var coordinator = Coordinator.init(true);
    const first = coordinator.submit(contract.op_search, .{});
    const job = switch (first) {
        .accepted => |ack| ack.job_id,
        else => return error.UnexpectedResult,
    };
    try std.testing.expect(switch (coordinator.submit(contract.op_download, .{})) {
        .busy => true,
        else => false,
    });
    try std.testing.expect(coordinator.takeWork() == null);
    try std.testing.expect(coordinator.markDurable(job, true));
    const work = coordinator.takeWork() orelse return error.MissingWork;
    try std.testing.expectEqual(job, work.job_id);
    try std.testing.expect(coordinator.transition(job, .searching, "catalog"));
    try std.testing.expect(coordinator.complete(job, .{ .state = .installed, .result = 0, .reason = "done" }));
    const status = coordinator.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@intFromEnum(contract.State.installed), status.state);
    try std.testing.expectEqualStrings("done", status.reasonText());
}

test "parallel callers produce exactly one accepted job" {
    var coordinator = Coordinator.init(true);
    const Runner = struct {
        fn run(ptr: *Coordinator) void {
            _ = ptr.submit(contract.op_search, .{});
        }
    };
    var threads: [8]std.Thread = undefined;
    for (&threads) |*thread| thread.* = try std.Thread.spawn(.{}, Runner.run, .{&coordinator});
    for (&threads) |*thread| thread.join();
    const status = coordinator.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@as(u32, 1), status.job_id);
    try std.testing.expectEqual(@intFromEnum(contract.State.queued), status.state);
}

test "cancel is job bound and restart turns active work into an interruption" {
    var coordinator = Coordinator.init(true);
    const accepted = coordinator.submit(contract.op_install, .{});
    const job = switch (accepted) {
        .accepted => |ack| ack.job_id,
        else => return error.UnexpectedResult,
    };
    try std.testing.expect(coordinator.markDurable(job, true));
    _ = coordinator.takeWork() orelse return error.MissingWork;
    try std.testing.expect(switch (coordinator.cancel(job + 1)) {
        .missing => true,
        else => false,
    });
    try std.testing.expect(switch (coordinator.cancel(job)) {
        .accepted => true,
        else => false,
    });
    try std.testing.expect(coordinator.cancelRequested());
    const saved = coordinator.snapshot() orelse return error.MissingStatus;

    var restarted = Coordinator.init(true);
    try std.testing.expect(restarted.restore(saved, true));
    const restored = restarted.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@intFromEnum(contract.State.interrupted), restored.state);
    try std.testing.expect((restored.flags & contract.flag_recovered) != 0);
}

test "snapshot-bound download reports progress and resumes after service restart" {
    var coordinator = Coordinator.init(true);
    const submitted = coordinator.submitDownload(.{ .search_job_id = 41, .result_index = 3 });
    const job = switch (submitted) {
        .accepted => |ack| ack.job_id,
        else => return error.UnexpectedResult,
    };
    try std.testing.expect(coordinator.markDurable(job, true));
    _ = coordinator.takeWork() orelse return error.MissingWork;
    try std.testing.expect(coordinator.progress(job, 4093, 8192));
    const saved = coordinator.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@as(u64, 4093), saved.progress_current);
    try std.testing.expectEqual(@as(u32, 41), saved.source_job_id);
    try std.testing.expectEqual(@as(u32, 3), saved.result_index);

    var restarted = Coordinator.init(true);
    try std.testing.expect(restarted.restore(saved, true));
    try std.testing.expect(restarted.recoverDownload(job));
    const recovered = restarted.takeWork() orelse return error.MissingWork;
    try std.testing.expectEqual(job, recovered.job_id);
    try std.testing.expectEqual(contract.Operation.download, recovered.operation);
}

test "a new job never inherits an old snapshot row" {
    var coordinator = Coordinator.init(true);
    const submitted = coordinator.submitDownload(.{ .search_job_id = 41, .result_index = 3 });
    const job = switch (submitted) {
        .accepted => |ack| ack.job_id,
        else => return error.UnexpectedResult,
    };
    try std.testing.expect(coordinator.markDurable(job, true));
    _ = coordinator.takeWork() orelse return error.MissingWork;
    try std.testing.expect(coordinator.complete(job, .{ .state = .downloaded, .result = 0, .reason = "done" }));

    const search = coordinator.submit(contract.op_search, .{});
    _ = switch (search) {
        .accepted => |ack| ack.job_id,
        else => return error.UnexpectedResult,
    };
    const status = coordinator.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@as(u32, 0), status.source_job_id);
    try std.testing.expectEqual(@as(u32, 0), status.result_index);
}

test "durable download completion wins the crash window before status commit" {
    var coordinator = Coordinator.init(true);
    const submitted = coordinator.submitDownload(.{ .search_job_id = 41, .result_index = 3 });
    const job = switch (submitted) {
        .accepted => |ack| ack.job_id,
        else => return error.UnexpectedResult,
    };
    try std.testing.expect(coordinator.markDurable(job, true));
    _ = coordinator.takeWork() orelse return error.MissingWork;
    try std.testing.expect(coordinator.progress(job, 8192, 8192));
    const saved = coordinator.snapshot() orelse return error.MissingStatus;

    var restarted = Coordinator.init(true);
    try std.testing.expect(restarted.restore(saved, true));
    try std.testing.expect(restarted.reconcileDownloadCompletion(job, .downloaded, contract.result_ok, 8192, 8192));
    const restored = restarted.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@intFromEnum(contract.State.downloaded), restored.state);
    try std.testing.expectEqual(@as(u32, 1), restored.completed_count);
}

test "Update All remains bound to one search snapshot and exposes serial queue progress" {
    var coordinator = Coordinator.init(true);
    const submitted = coordinator.submitSnapshot(.update_all, .{ .search_job_id = 73 });
    const job = switch (submitted) {
        .accepted => |ack| ack.job_id,
        else => return error.UnexpectedResult,
    };
    try std.testing.expect(coordinator.markDurable(job, true));
    const work = coordinator.takeWork() orelse return error.MissingWork;
    try std.testing.expectEqual(contract.Operation.update_all, work.operation);
    try std.testing.expectEqual(@as(u32, 73), work.source_job_id);
    try std.testing.expect(coordinator.transitionPackage(job, .verifying, 73, 2, 5, 2, "verify"));
    const status = coordinator.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@as(u32, 73), status.source_job_id);
    try std.testing.expectEqual(@as(u32, 2), status.result_index);
    try std.testing.expectEqual(@as(u32, 5), status.package_count);
    try std.testing.expectEqual(@as(u32, 2), status.completed_count);
    try std.testing.expectEqual(@intFromEnum(contract.State.verifying), status.state);
    try std.testing.expect(!coordinator.transitionPackage(job, .installing, 73, 3, 5, 6, "invalid"));
}

test "unknown operations and malformed requests fail without allocating a job" {
    var coordinator = Coordinator.init(true);
    try std.testing.expect(switch (coordinator.submit(0xFFFF, .{})) {
        .invalid => true,
        else => false,
    });
    var malformed = contract.CommandRequest{};
    malformed.header.version +%= 1;
    try std.testing.expect(switch (coordinator.submit(contract.op_search, malformed)) {
        .invalid => true,
        else => false,
    });
    const status = coordinator.snapshot() orelse return error.MissingStatus;
    try std.testing.expectEqual(@as(u32, 0), status.job_id);
}

test "endpoint lifecycle keeps multiple clients on one coordinator" {
    const Harness = struct {
        registered: bool = false,
        next_handle: u32 = 0,
        open_mask: u32 = 0,
        coordinator: Coordinator = Coordinator.init(true),

        fn register(self: *@This()) bool {
            if (self.registered) return false;
            self.registered = true;
            return true;
        }

        fn open(self: *@This()) ?u32 {
            if (!self.registered or self.next_handle >= 31) return null;
            self.next_handle += 1;
            self.open_mask |= @as(u32, 1) << @intCast(self.next_handle);
            return self.next_handle;
        }

        fn call(self: *@This(), handle: u32, operation: u16) SubmitResult {
            if ((self.open_mask & (@as(u32, 1) << @intCast(handle))) == 0)
                return .{ .invalid = makeAck(0, operation, .failed, contract.result_invalid, 0) };
            return self.coordinator.submit(operation, .{});
        }

        fn close(self: *@This(), handle: u32) bool {
            const bit = @as(u32, 1) << @intCast(handle);
            if ((self.open_mask & bit) == 0) return false;
            self.open_mask &= ~bit;
            return true;
        }
    };

    var harness = Harness{};
    try std.testing.expect(harness.register());
    try std.testing.expect(!harness.register());
    const first = harness.open() orelse return error.OpenFailed;
    const second = harness.open() orelse return error.OpenFailed;
    try std.testing.expect(switch (harness.call(first, contract.op_search)) {
        .accepted => true,
        else => false,
    });
    try std.testing.expect(switch (harness.call(second, contract.op_download)) {
        .busy => true,
        else => false,
    });
    try std.testing.expect(harness.close(first));
    try std.testing.expect(!harness.close(first));
    try std.testing.expect(switch (harness.call(first, contract.op_search)) {
        .invalid => true,
        else => false,
    });
    try std.testing.expect(harness.close(second));
}
