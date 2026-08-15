const std = @import("std");
const r4os = @import("r4os");
const r4std = @import("r4std");
const contract = @import("update_service_contract");
const core = @import("core.zig");
const update_engine = @import("system_update_engine");
const update_catalog = r4os.update_catalog;
const update_download = @import("update_download.zig");

const service_args = "/RUN";
const selftest_arg = "/SELFTEST";
const update_root: [*:0]const u8 = "C:\\R4OS\\UPDATE";
const staged_root: [*:0]const u8 = "C:\\R4OS\\UPDATE\\STAGED";
const inbox_root: [*:0]const u8 = "C:\\R4OS\\UPDATE\\INBOX";
const inbox_prefix = "C:\\R4OS\\UPDATE\\INBOX\\";
const worker_stack_bytes: u64 = 512 * 1024;
const endpoint_wait_ns: u64 = 1_000_000;
const config_capacity: usize = 4096;
const state_capacity: usize = 2048;
const release_capacity: usize = 1024;
const raw_response_capacity: usize = update_catalog.max_catalog_bytes + r4os.http.max_header_bytes;
const download_io_capacity: usize = 16 * 1024;
const download_checkpoint_bytes: u64 = 256 * 1024;

const ServiceConfig = struct {
    valid: bool = false,
    enabled: bool = true,
    server: [64]u8 = .{0} ** 64,
    server_len: usize = 0,
    auth_port: u16 = 0,
    update_port: u16 = 0,
    catalog_url: [update_catalog.max_download_url_bytes]u8 = .{0} ** update_catalog.max_download_url_bytes,
    catalog_url_len: usize = 0,
    magic_key: [34]u8 = .{0} ** 34,
    magic_key_len: usize = 0,
    channel: [32]u8 = .{0} ** 32,
    channel_len: usize = 0,
    connect_timeout_ms: u32 = 5000,
    request_timeout_ms: u32 = 30000,
    allow_redirects: bool = false,
};

const SearchResults = struct {
    job_id: u32 = 0,
    catalog_len: usize = 0,
    inventory_len: usize = 0,
    release_len: usize = 0,
    current_release_len: usize = 0,
    active_kernel_len: usize = 0,
    catalog_bytes: [update_catalog.max_catalog_bytes]u8 = undefined,
    raw_response: [raw_response_capacity]u8 = undefined,
    tls_scratch: [r4os.app_web.tls_scratch_bytes]u8 = undefined,
    inventory_bytes: [r4os.system_update_inventory.max_bytes]u8 = undefined,
    release_bytes: [release_capacity]u8 = undefined,
    current_release: [contract.offer_release_capacity]u8 = .{0} ** contract.offer_release_capacity,
    active_kernel: [contract.component_version_capacity]u8 = .{0} ** contract.component_version_capacity,
    release: update_catalog.CatalogRelease = .{},
    parse_workspace: update_catalog.CatalogRelease = .{},
    inventory: r4os.system_update_inventory.Inventory = .{},
    plan: update_catalog.Plan = .{},
    offer_states: [update_catalog.max_packages]u16 = .{@intFromEnum(contract.State.available)} ** update_catalog.max_packages,
    offer_results: [update_catalog.max_packages]i32 = .{contract.result_ok} ** update_catalog.max_packages,
    offer_progress: [update_catalog.max_packages]u64 = .{0} ** update_catalog.max_packages,
};

const DownloadRuntime = struct {
    valid: bool = false,
    record: update_download.Record = .{},
    header: [r4os.http.max_header_bytes]u8 = undefined,
    io: [download_io_capacity]u8 = undefined,
    scratch: [r4os.app_web.tls_scratch_bytes]u8 = undefined,
    hash_scratch: [download_io_capacity]u8 = undefined,
};

const Runtime = struct {
    app: ?*r4os.App = null,
    coordinator: core.Coordinator = .{},
    config: ServiceConfig = .{},
    stop: r4os.app_contract.StopFlag = .{},
    shutdown: u32 = 0,
    persist_lock: u32 = 0,
    results_lock: u32 = 0,
    results: SearchResults = .{},
    download: DownloadRuntime = .{},
    prepared_restart: bool = false,
    prepared_search_job_id: u32 = 0,
};

var runtime: Runtime = .{};

pub fn r4_app_main(app: *r4os.App) i32 {
    if (!r4std.init(app.startContext())) return r4os.abi.err_no_group;
    if (hasArg(app.args(), selftest_arg)) return runSelfTest(app);
    return runService(app);
}

fn runService(app: *r4os.App) i32 {
    const ctx = app.system();
    const services = app.services() orelse return r4os.abi.service_api_result_invalid;
    const resources = app.resources();
    if (!resources.available()) return r4os.abi.thread_error_unsupported;

    _ = ctx.dirCreate(update_root);
    _ = ctx.dirCreate(staged_root);
    _ = ctx.dirCreate(inbox_root);
    finishPostBootBatch(app);
    const loaded_config = loadConfig(&ctx);
    runtime.app = app;
    runtime.coordinator = core.Coordinator.init(loaded_config.valid);
    runtime.config = loaded_config;
    runtime.stop = .{};
    runtime.shutdown = 0;
    runtime.persist_lock = 0;
    runtime.results_lock = 0;
    runtime.results.job_id = 0;
    runtime.download.valid = loadDownloadRecord(&ctx, &runtime.download.record);
    if (loadStatus(&ctx)) |saved| {
        if (runtime.coordinator.restore(saved, loaded_config.valid) and runtime.download.valid and
            runtime.download.record.job_id == saved.job_id) switch (runtime.download.record.state) {
            .downloading => _ = runtime.coordinator.recoverDownload(saved.job_id),
            .downloaded => _ = runtime.coordinator.reconcileDownloadCompletion(
                saved.job_id,
                .downloaded,
                runtime.download.record.result,
                runtime.download.record.progress,
                runtime.download.record.expected_size,
            ),
            .failed => _ = runtime.coordinator.reconcileDownloadCompletion(
                saved.job_id,
                .failed,
                runtime.download.record.result,
                runtime.download.record.progress,
                runtime.download.record.expected_size,
            ),
            .available => {},
        };
    }
    if (snapshotBlocking(&ctx)) |status| _ = persistStatus(&ctx, status);

    var worker = switch (resources.createThread(workerMain, @intFromPtr(&runtime), worker_stack_bytes)) {
        .handle => |value| value,
        .failure => |raw| {
            ctx.write("UPDSVC worker creation failed rc=");
            ctx.printI32(raw);
            ctx.println("");
            return raw;
        },
    };

    var endpoint = registerEndpoint(&ctx, &services) orelse {
        @atomicStore(u32, &runtime.shutdown, 1, .release);
        _ = worker.join(r4os.time_contract.timeoutForever());
        return r4os.abi.service_api_result_no_endpoint;
    };

    ctx.println("UPDSVC ready endpoint=UPDSVC worker=1 contract=1");
    while (!ctx.programShouldClose()) {
        switch (endpoint.wait(r4os.time_contract.timeoutFinite(r4os.time_contract.durationFromNanoseconds(endpoint_wait_ns)))) {
            .ready => {
                const rc = handleRequest(&ctx, &endpoint);
                if (rc < 0) {
                    _ = endpoint.unregister();
                    @atomicStore(u32, &runtime.shutdown, 1, .release);
                    runtime.stop.request();
                    _ = worker.join(r4os.time_contract.timeoutForever());
                    return rc;
                }
            },
            .timed_out => {},
            .failure => |raw| {
                @atomicStore(u32, &runtime.shutdown, 1, .release);
                runtime.stop.request();
                _ = worker.join(r4os.time_contract.timeoutForever());
                return raw;
            },
        }
    }

    runtime.coordinator.stop();
    runtime.stop.request();
    @atomicStore(u32, &runtime.shutdown, 1, .release);
    if (snapshotBlocking(&ctx)) |status| _ = persistStatus(&ctx, status);
    _ = endpoint.unregister();
    _ = worker.join(r4os.time_contract.timeoutForever());
    ctx.println("UPDSVC stopped cleanly");
    return 0;
}

fn finishPostBootBatch(app: *r4os.App) void {
    var engine = update_engine.Engine.init(app);
    const status = engine.status();
    if (status.exit_code != 0 or (status.state != .pending_restart and status.state != .restart_required)) return;
    const resumed = engine.resumeBatch();
    if (resumed.exit_code != 0) {
        const ctx = app.system();
        ctx.write("UPDSVC post-boot update finalization failed: ");
        ctx.println(resumed.reasonText());
    }
}

fn registerEndpoint(ctx: *const r4os.r4sys.Context, services: *const r4os.Services) ?r4os.ServiceEndpoint {
    var waited: u32 = 0;
    while (waited < 100) : (waited += 1) {
        switch (services.register(contract.service_name, 0)) {
            .endpoint => |value| return value,
            .failure => {},
        }
        ctx.sleepTicks(1);
    }
    return null;
}

fn handleRequest(ctx: *const r4os.r4sys.Context, endpoint: *r4os.ServiceEndpoint) i32 {
    var payload: [r4os.abi.service_api_max_payload]u8 = undefined;
    const message = switch (endpoint.recv(payload[0..])) {
        .message => |value| value,
        .would_block => return 0,
        .failure => |raw| return raw,
    };
    const body = payload[0..message.bytes];
    return switch (message.header.op) {
        contract.op_status => replyStatus(ctx, endpoint, message.header.request_id, body),
        contract.op_search, contract.op_install => replySubmit(ctx, endpoint, message.header.request_id, message.header.op, body),
        contract.op_update_all, contract.op_restart => replySnapshotSubmit(ctx, endpoint, message.header.request_id, message.header.op, body),
        contract.op_download => replyDownloadSubmit(ctx, endpoint, message.header.request_id, body),
        contract.op_cancel => replyCancel(ctx, endpoint, message.header.request_id, body),
        contract.op_results => replyResults(ctx, endpoint, message.header.request_id, body),
        contract.op_components => replyComponents(ctx, endpoint, message.header.request_id, body),
        else => endpoint.reply(message.header.request_id, r4os.abi.service_api_result_bad_op, ""),
    };
}

fn replyStatus(
    ctx: *const r4os.r4sys.Context,
    endpoint: *r4os.ServiceEndpoint,
    request_id: u32,
    body: []const u8,
) i32 {
    const request = decodeStruct(contract.StatusRequest, body) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!request.header.valid(@sizeOf(contract.StatusRequest)))
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    var status = runtime.coordinator.snapshot() orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_busy, "");
    if (request.job_id != 0 and request.job_id != status.job_id)
        return endpoint.reply(request_id, r4os.abi.service_api_result_not_found, "");
    if (runtime.download.valid and (status.operation == contract.op_download or status.operation == contract.op_install)) {
        status.source_job_id = runtime.download.record.search_job_id;
        status.result_index = runtime.download.record.result_index;
    }
    _ = ctx;
    return endpoint.replyTyped(contract.Status, request_id, r4os.abi.service_api_result_ok, &status);
}

fn replySnapshotSubmit(
    ctx: *const r4os.r4sys.Context,
    endpoint: *r4os.ServiceEndpoint,
    request_id: u32,
    operation_raw: u16,
    body: []const u8,
) i32 {
    const request = decodeStruct(contract.SnapshotRequest, body) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!request.header.valid(@sizeOf(contract.SnapshotRequest)) or request.search_job_id == 0)
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    const operation = contract.operationFromWire(operation_raw) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_bad_op, "");
    if (!tryAcquireResultsLock()) return endpoint.reply(request_id, r4os.abi.service_api_result_busy, "");
    defer releaseResultsLock();
    if (runtime.results.job_id != request.search_job_id)
        return replySnapshotFailure(endpoint, request_id, operation_raw, contract.result_selection_stale);
    if (operation == .update_all and runtime.results.plan.package_count == 0)
        return replySnapshotFailure(endpoint, request_id, operation_raw, contract.result_not_ready);
    if (operation == .restart and (!runtime.prepared_restart or runtime.prepared_search_job_id != request.search_job_id))
        return replySnapshotFailure(endpoint, request_id, operation_raw, contract.result_not_ready);

    const submitted = runtime.coordinator.submitSnapshot(operation, request);
    var ack = switch (submitted) {
        .accepted => |value| value,
        .busy => |value| return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &value),
        .invalid => |value| return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &value),
    };
    runtime.stop = .{};
    const queued = snapshotBlocking(ctx) orelse {
        _ = markDurableBlocking(ctx, ack.job_id, false);
        ack.result = contract.result_persist_failed;
        ack.state = @intFromEnum(contract.State.failed);
        return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
    };
    const persisted = persistStatus(ctx, queued);
    if (!markDurableBlocking(ctx, ack.job_id, persisted)) {
        ack.result = contract.result_persist_failed;
        ack.state = @intFromEnum(contract.State.failed);
    } else if (runtime.coordinator.snapshot()) |durable| {
        ack.generation = durable.generation;
        ack.state = durable.state;
    }
    return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
}

fn replySnapshotFailure(endpoint: *r4os.ServiceEndpoint, request_id: u32, operation: u16, result: i32) i32 {
    const ack = contract.Ack{
        .operation = operation,
        .state = @intFromEnum(contract.State.failed),
        .result = result,
    };
    return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
}

fn replySubmit(
    ctx: *const r4os.r4sys.Context,
    endpoint: *r4os.ServiceEndpoint,
    request_id: u32,
    operation: u16,
    body: []const u8,
) i32 {
    const request = decodeStruct(contract.CommandRequest, body) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    const submitted = runtime.coordinator.submit(operation, request);
    var ack = switch (submitted) {
        .accepted => |value| value,
        .busy => |value| return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &value),
        .invalid => |value| return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &value),
    };

    runtime.stop = .{};
    const queued = snapshotBlocking(ctx) orelse {
        _ = markDurableBlocking(ctx, ack.job_id, false);
        ack.result = contract.result_persist_failed;
        ack.state = @intFromEnum(contract.State.failed);
        return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
    };
    const persisted = persistStatus(ctx, queued);
    if (!markDurableBlocking(ctx, ack.job_id, persisted)) {
        ack.result = contract.result_persist_failed;
        ack.state = @intFromEnum(contract.State.failed);
    } else if (runtime.coordinator.snapshot()) |durable| {
        ack.generation = durable.generation;
        ack.state = durable.state;
    }
    return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
}

fn replyDownloadSubmit(
    ctx: *const r4os.r4sys.Context,
    endpoint: *r4os.ServiceEndpoint,
    request_id: u32,
    body: []const u8,
) i32 {
    const request = decodeStruct(contract.DownloadRequest, body) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!request.header.valid(@sizeOf(contract.DownloadRequest)) or request.search_job_id == 0)
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!tryAcquireResultsLock())
        return endpoint.reply(request_id, r4os.abi.service_api_result_busy, "");
    defer releaseResultsLock();

    if (runtime.results.job_id != request.search_job_id or request.result_index >= runtime.results.plan.package_count) {
        const stale = contract.Ack{
            .operation = contract.op_download,
            .state = @intFromEnum(contract.State.failed),
            .result = contract.result_selection_stale,
        };
        return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &stale);
    }

    var offer = contract.Offer{};
    if (!fillOffer(&offer, request.result_index))
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (offer.state == @intFromEnum(contract.State.downloaded)) {
        const ready = contract.Ack{
            .job_id = if (runtime.download.valid) runtime.download.record.job_id else 0,
            .operation = contract.op_download,
            .state = @intFromEnum(contract.State.downloaded),
            .result = contract.result_ok,
        };
        return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ready);
    }

    const submitted = runtime.coordinator.submitDownload(request);
    var ack = switch (submitted) {
        .accepted => |value| value,
        .busy => |value| return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &value),
        .invalid => |value| return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &value),
    };
    const candidate = update_download.Record.init(
        ack.job_id,
        request.search_job_id,
        request.result_index,
        offer.package_id[0..offer.package_id_len],
        offer.package_version[0..offer.package_version_len],
        offer.release[0..offer.release_len],
        offer.filename[0..offer.filename_len],
        offer.sha256[0..offer.sha256_len],
        offer.download_url[0..offer.download_url_len],
        offer.size_bytes,
    ) orelse {
        _ = markDurableBlocking(ctx, ack.job_id, false);
        ack.state = @intFromEnum(contract.State.failed);
        ack.result = contract.result_invalid;
        return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
    };

    if (runtime.download.valid and !runtime.download.record.sameOffer(&candidate))
        discardPart(ctx, &runtime.download.record);
    runtime.download.record = candidate;
    runtime.download.valid = true;
    runtime.results.offer_states[request.result_index] = @intFromEnum(contract.State.downloading);
    runtime.results.offer_results[request.result_index] = contract.result_ok;
    runtime.results.offer_progress[request.result_index] = 0;
    runtime.stop = .{};

    const queued = snapshotBlocking(ctx);
    const persisted = persistDownloadRecord(ctx, &runtime.download.record) and
        (if (queued) |status| persistStatus(ctx, status) else false);
    if (!markDurableBlocking(ctx, ack.job_id, persisted)) {
        ack.result = contract.result_persist_failed;
        ack.state = @intFromEnum(contract.State.failed);
        runtime.results.offer_states[request.result_index] = @intFromEnum(contract.State.failed);
        runtime.results.offer_results[request.result_index] = contract.result_persist_failed;
    } else if (runtime.coordinator.snapshot()) |durable| {
        ack.generation = durable.generation;
        ack.state = durable.state;
    }
    return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
}

fn replyCancel(
    ctx: *const r4os.r4sys.Context,
    endpoint: *r4os.ServiceEndpoint,
    request_id: u32,
    body: []const u8,
) i32 {
    const request = decodeStruct(contract.CancelRequest, body) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!request.header.valid(@sizeOf(contract.CancelRequest)))
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    const result = runtime.coordinator.cancel(request.job_id);
    var ack = switch (result) {
        .accepted => |value| value,
        .missing => |value| value,
        .busy => |value| value,
    };
    if (ack.result == contract.result_ok) {
        runtime.stop.request();
        if (snapshotBlocking(ctx)) |status| {
            if (!persistStatus(ctx, status)) ack.result = contract.result_persist_failed;
        }
    }
    return endpoint.replyTyped(contract.Ack, request_id, r4os.abi.service_api_result_ok, &ack);
}

fn replyResults(ctx: *const r4os.r4sys.Context, endpoint: *r4os.ServiceEndpoint, request_id: u32, body: []const u8) i32 {
    const request = decodeStruct(contract.ResultsRequest, body) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!request.header.valid(@sizeOf(contract.ResultsRequest)))
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!tryAcquireResultsLock())
        return endpoint.reply(request_id, r4os.abi.service_api_result_busy, "");
    defer releaseResultsLock();
    if (runtime.results.job_id == 0)
        return endpoint.reply(request_id, r4os.abi.service_api_result_not_found, "");
    if (request.job_id != 0 and request.job_id != runtime.results.job_id)
        return endpoint.reply(request_id, r4os.abi.service_api_result_not_found, "");

    var page = contract.ResultsPage{
        .job_id = runtime.results.job_id,
        .total = @intCast(runtime.results.plan.package_count),
        .index = request.index,
        .flags = contract.flag_results_ready |
            (if (runtime.results.plan.restart_required) contract.flag_restart_required else 0),
    };
    page.current_release_len = @intCast(copyFixed(page.current_release[0..], runtime.results.current_release[0..runtime.results.current_release_len]));
    if (runtime.coordinator.snapshot()) |status| {
        if (status.job_id == runtime.results.job_id) page.generation = status.generation;
    }
    if (request.index < runtime.results.plan.package_count) {
        if (!fillOffer(&page.offer, request.index)) {
            page.result = contract.result_catalog_invalid;
        } else {
            page.has_offer = 1;
        }
    }
    _ = ctx;
    return endpoint.replyTyped(contract.ResultsPage, request_id, r4os.abi.service_api_result_ok, &page);
}

fn replyComponents(ctx: *const r4os.r4sys.Context, endpoint: *r4os.ServiceEndpoint, request_id: u32, body: []const u8) i32 {
    const request = decodeStruct(contract.ComponentRequest, body) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!request.header.valid(@sizeOf(contract.ComponentRequest)))
        return endpoint.reply(request_id, r4os.abi.service_api_result_invalid, "");
    if (!tryAcquireResultsLock())
        return endpoint.reply(request_id, r4os.abi.service_api_result_busy, "");
    defer releaseResultsLock();
    if (runtime.results.job_id == 0 or request.job_id != runtime.results.job_id or
        request.result_index >= runtime.results.plan.package_count)
        return endpoint.reply(request_id, r4os.abi.service_api_result_not_found, "");

    const package_index = runtime.results.plan.packageIndex(request.result_index) orelse
        return endpoint.reply(request_id, r4os.abi.service_api_result_not_found, "");
    if (package_index >= runtime.results.release.package_count)
        return endpoint.reply(request_id, r4os.abi.service_api_result_not_found, "");
    const package = &runtime.results.release.packages[package_index];
    var page = contract.ComponentPage{
        .job_id = runtime.results.job_id,
        .result_index = request.result_index,
        .component_index = request.component_index,
        .total = package.component_count,
    };
    if (request.component_index < package.component_count) {
        if (fillComponent(&page.component, package, request.component_index)) {
            page.has_component = 1;
        } else {
            page.result = contract.result_catalog_invalid;
        }
    }
    _ = ctx;
    return endpoint.replyTyped(contract.ComponentPage, request_id, r4os.abi.service_api_result_ok, &page);
}

fn workerMain(arg: u64) callconv(.c) i32 {
    const state: *Runtime = @ptrFromInt(arg);
    const app = state.app orelse return 1;
    const ctx = app.system();
    while (@atomicLoad(u32, &state.shutdown, .acquire) == 0) {
        if (state.coordinator.takeWork()) |work| {
            if (state.coordinator.snapshot()) |status| _ = persistStatus(&ctx, status);
            const completion = executeWork(app, state, work);
            _ = state.coordinator.complete(work.job_id, completion);
            if (state.coordinator.snapshot()) |status| _ = persistStatus(&ctx, status);
        } else {
            ctx.sleepTicks(1);
        }
    }
    return 0;
}

fn executeWork(app: *r4os.App, state: *Runtime, work: core.Work) core.Completion {
    if (state.coordinator.cancelRequested() or state.stop.requested()) return cancelledCompletion();
    return switch (work.operation) {
        .install => executeInstall(app, state, &work),
        .search => executeSearch(app, state, &work),
        .download => executeDownload(app, state, &work),
        .update_all => executeUpdateAll(app, state, &work),
        .restart => executeRestart(app, state, &work),
    };
}

fn executeSearch(app: *r4os.App, state: *Runtime, work: *const core.Work) core.Completion {
    const ctx = app.system();
    if (!state.config.valid) return searchFailure(contract.result_invalid, "config-invalid");
    if (!tryAcquireResultsLock()) return searchFailure(contract.result_busy, "results-busy");
    var results_locked = true;
    defer if (results_locked) releaseResultsLock();
    state.results.job_id = 0;
    state.results.catalog_len = 0;
    state.results.inventory_len = 0;
    state.results.release_len = 0;
    state.results.current_release_len = 0;
    state.results.active_kernel_len = 0;
    state.results.release = .{};
    state.results.parse_workspace = .{};
    state.results.plan = .{};
    @memset(state.results.offer_states[0..], @intFromEnum(contract.State.available));
    @memset(state.results.offer_results[0..], contract.result_ok);
    @memset(state.results.offer_progress[0..], 0);

    if (state.coordinator.cancelRequested() or state.stop.requested()) return cancelledCompletion();
    switch (authenticate(app, &state.config)) {
        .ok => {},
        .cancelled => return cancelledCompletion(),
        .no_response => return searchFailure(contract.result_auth_failed, "auth-no-response"),
        .bad_response => return searchFailure(contract.result_auth_failed, "auth-invalid-response"),
        .network_failure => return searchFailure(contract.result_network_failed, "auth-network-failed"),
    }
    if (state.coordinator.cancelRequested() or state.stop.requested()) return cancelledCompletion();
    if (state.coordinator.transition(work.job_id, .searching, "catalog")) {
        if (state.coordinator.snapshot()) |status| _ = persistStatus(&ctx, status);
    }

    const release_read = ctx.fileRead("C:\\R4OS\\CONFIG\\VERSION.R4S", state.results.release_bytes[0..]);
    if (release_read <= 0 or release_read > @as(i32, @intCast(state.results.release_bytes.len)))
        return searchFailure(contract.result_inventory_invalid, "release-missing");
    state.results.release_len = @intCast(release_read);
    const local_release = r4os.version_info.parseReleaseVersion(state.results.release_bytes[0..state.results.release_len]) orelse
        return searchFailure(contract.result_inventory_invalid, "release-invalid");
    state.results.current_release_len = copyFixed(state.results.current_release[0..], local_release);

    const inventory_read = ctx.fileRead("C:\\R4OS\\CONFIG\\MODULES.JSON", state.results.inventory_bytes[0..]);
    if (inventory_read <= 0 or inventory_read > @as(i32, @intCast(state.results.inventory_bytes.len)))
        return searchFailure(contract.result_inventory_invalid, "inventory-missing");
    state.results.inventory_len = @intCast(inventory_read);
    if (!r4os.system_update_inventory.Inventory.parse(
        state.results.inventory_bytes[0..state.results.inventory_len],
        &state.results.inventory,
    )) return searchFailure(contract.result_inventory_invalid, "inventory-invalid");

    var active_buffer: [r4os.r4u_manifest.version_max_bytes]u8 = undefined;
    const devices = app.devicesLowLevel() orelse return searchFailure(contract.result_inventory_invalid, "kernel-version-unavailable");
    const active_value = devices.kernelVersion() orelse return searchFailure(contract.result_inventory_invalid, "kernel-version-unavailable");
    const active_kernel = r4os.version_info.formatKernelVersion(active_value, active_buffer[0..]) orelse
        return searchFailure(contract.result_inventory_invalid, "kernel-version-invalid");
    state.results.active_kernel_len = copyFixed(state.results.active_kernel[0..], active_kernel);

    var web = app.web() orelse return searchFailure(contract.result_network_failed, "web-unavailable");
    const fetch = web.fetch(
        state.config.catalog_url[0..state.config.catalog_url_len],
        state.results.raw_response[0..],
        state.results.catalog_bytes[0..],
        state.results.tls_scratch[0..],
        .{
            .timeout = timeoutFromMs(state.config.request_timeout_ms),
            .redirect = if (state.config.allow_redirects) .follow else .error_mode,
            .redirect_limit = if (state.config.allow_redirects) 3 else 0,
            .stop = &state.stop.storage,
            .headers = "accept:application/json\n",
        },
    );
    const response = switch (fetch) {
        .response => |value| value,
        .failure => |failure| return if (failure == .cancelled)
            cancelledCompletion()
        else if (failure == .response_too_large or failure == .header_buffer_too_small or failure == .io_buffer_too_small)
            searchFailure(contract.result_response_too_large, "catalog-too-large")
        else
            searchFailure(contract.result_network_failed, @tagName(failure)),
    };
    if (response.status == 401) {
        const warning = std.mem.trim(u8, response.body, " \t\r\n");
        if (textEquals(warning, "STOP NOW!")) return searchFailure(contract.result_access_warning, "STOP NOW!");
        if (textEquals(warning, "LAST CHANCE!")) return searchFailure(contract.result_access_warning, "LAST CHANCE!");
        return searchFailure(contract.result_auth_failed, "authenticate-required");
    }
    if (response.status != 200) return searchFailure(contract.result_network_failed, "catalog-http-status");
    if (response.content_type) |content_type| {
        if (!startsWithIgnoreCase(content_type, "application/json"))
            return searchFailure(contract.result_catalog_invalid, "catalog-content-type");
    }
    state.results.catalog_len = response.body.len;

    var depth: [32]u8 = undefined;
    var json_document = r4os.json.Document.init(&devices, response.body, depth[0..]);
    update_catalog.parseDocument(
        &json_document,
        state.results.inventory.profile,
        local_release,
        &state.results.parse_workspace,
        &state.results.release,
    ) catch |failure| switch (failure) {
        error.NoEligibleRelease => {
            state.results.plan.restart_required = r4os.version_info.restartRequired(active_value, installedKernel(&state.results.inventory) orelse active_kernel);
            state.results.job_id = work.job_id;
            return searchCompletion(&state.results.plan);
        },
        error.LimitExceeded => return searchFailure(contract.result_response_too_large, "catalog-limit"),
        else => return searchFailure(contract.result_catalog_invalid, "catalog-invalid"),
    };

    state.results.plan = update_catalog.buildPlan(
        &state.results.release,
        &state.results.inventory,
        local_release,
        active_kernel,
    ) catch |failure| switch (failure) {
        error.UnmetRequirement, error.DependencyOrder => return searchFailure(contract.result_requirement_unmet, "requirement-unmet"),
        error.TooManyPackages => return searchFailure(contract.result_response_too_large, "too-many-packages"),
        error.InvalidInventory => return searchFailure(contract.result_inventory_invalid, "inventory-invalid"),
        else => return searchFailure(contract.result_catalog_invalid, "catalog-plan-invalid"),
    };
    hydrateDownloadedOffers(&ctx, state);
    state.results.job_id = work.job_id;
    if (state.results.plan.package_count == 0 and state.results.plan.release_newer and
        !state.results.plan.restart_required)
    {
        // A crash or service restart may happen after the last live package
        // committed its artifact and inventory but before VERSION.R4S was
        // confirmed. A later search proves the same complete catalog target
        // and closes that durable gap instead of reporting a split state.
        releaseResultsLock();
        results_locked = false;
        var engine = update_engine.Engine.init(app);
        const confirmed = engine.confirmLiveRelease(local_release, state.results.release.release);
        if (confirmed.exit_code != 0)
            return searchFailure(confirmed.exit_code, confirmed.reasonText());
    }
    return searchCompletion(&state.results.plan);
}

const AuthResult = enum { ok, cancelled, no_response, bad_response, network_failure };

fn authenticate(app: *r4os.App, config: *const ServiceConfig) AuthResult {
    if (runtime.stop.requested()) return .cancelled;
    const ctx = app.system();
    const network = app.network() orelse return .network_failure;
    const server = config.server[0..config.server_len];
    const address = parseIpv4(server) orelse block: {
        var resolver = network.resolver();
        break :block switch (resolver.resolveA(server, null, timeoutFromMs(config.connect_timeout_ms))) {
            .address => |value| value,
            .timed_out, .not_found, .no_service, .failure => return .network_failure,
        };
    };
    var socket = switch (network.connectTcp(.{ .address = address, .port = config.auth_port }, timeoutFromMs(config.connect_timeout_ms))) {
        .socket => |value| value,
        .timed_out, .would_block, .reset, .peer_closed, .no_service, .failure => return .network_failure,
    };
    defer _ = socket.close(timeoutFromMs(config.connect_timeout_ms));

    const key = config.magic_key[0..config.magic_key_len];
    var auth_line: [35]u8 = undefined;
    if (key.len + 1 > auth_line.len) return .network_failure;
    @memcpy(auth_line[0..key.len], key);
    auth_line[key.len] = '\n';
    const payload = auth_line[0 .. key.len + 1];
    var sent: usize = 0;
    while (sent < payload.len) {
        if (runtime.stop.requested()) return .cancelled;
        switch (socket.write(payload[sent..], timeoutFromMs(config.connect_timeout_ms))) {
            .bytes => |count| {
                if (count == 0 or count > payload.len - sent) return .network_failure;
                sent += count;
            },
            else => return .network_failure,
        }
    }

    var reply: [2]u8 = undefined;
    var received: usize = 0;
    const reply_started = ctx.timeState().monotonic_ticks;
    while (received < reply.len) {
        if (runtime.stop.requested()) return .cancelled;
        switch (socket.read(reply[received..], timeoutFromMs(config.connect_timeout_ms))) {
            .bytes => |count| {
                if (count > reply.len - received) return .bad_response;
                if (count == 0) {
                    if (authReplyExpired(&ctx, reply_started, config.connect_timeout_ms)) return .no_response;
                    ctx.sleepTicks(1);
                    continue;
                }
                received += count;
            },
            .would_block, .timed_out => {
                if (authReplyExpired(&ctx, reply_started, config.connect_timeout_ms)) return .no_response;
                ctx.sleepTicks(1);
                continue;
            },
            .peer_closed => return .no_response,
            else => return .network_failure,
        }
    }
    return if (textEquals(reply[0..], "OK")) .ok else .bad_response;
}

fn authReplyExpired(ctx: *const r4os.r4sys.Context, started: u64, timeout_ms: u32) bool {
    const elapsed = ctx.timeState().monotonic_ticks -| started;
    return elapsed >= ctx.ticksFromMilliseconds(timeout_ms);
}

fn searchCompletion(plan: *const update_catalog.Plan) core.Completion {
    const state: contract.State = if (plan.package_count != 0)
        .available
    else if (plan.restart_required)
        .pending_restart
    else
        .up_to_date;
    return .{
        .state = state,
        .result = contract.result_ok,
        .flags = contract.flag_results_ready |
            (if (plan.restart_required) contract.flag_restart_required else 0),
        .package_count = @intCast(plan.package_count),
        .reason = contract.stateName(@intFromEnum(state)),
    };
}

fn searchFailure(result: i32, reason: []const u8) core.Completion {
    return .{ .state = .failed, .result = result, .reason = reason };
}

const DownloadPaths = struct {
    part: r4os.AbsoluteFilePath,
    final: r4os.AbsoluteFilePath,
    backup: r4os.AbsoluteFilePath,
};

const DownloadFlow = struct {
    ctx: *const r4os.r4sys.Context,
    state: *Runtime,
    paths: *const DownloadPaths,
    job_id: u32,
    current: u64,
    last_checkpoint: u64,
    persist_failed: bool = false,

    fn write(raw: ?*anyopaque, offset: u64, bytes: []const u8) bool {
        const self: *DownloadFlow = @ptrCast(@alignCast(raw orelse return false));
        if (offset != self.current or bytes.len == 0 or self.current + bytes.len > self.state.download.record.expected_size)
            return false;
        const written = self.ctx.fileAppend(self.paths.part.asZ().ptr, bytes);
        if (written < 0 or @as(usize, @intCast(written)) != bytes.len) return false;
        self.current += bytes.len;
        self.state.download.record.progress = self.current;
        return true;
    }

    fn progress(raw: ?*anyopaque, current: u64, total: u64) bool {
        const self: *DownloadFlow = @ptrCast(@alignCast(raw orelse return false));
        if (current != self.current or total != self.state.download.record.expected_size) return false;
        _ = self.state.coordinator.progress(self.job_id, current, total);
        updateOfferState(self.ctx, &self.state.download.record, .downloading, contract.result_ok, current);
        if (current == total or current - self.last_checkpoint >= download_checkpoint_bytes) {
            self.last_checkpoint = current;
            const status = snapshotBlocking(self.ctx);
            if (!persistDownloadRecord(self.ctx, &self.state.download.record) or
                (status != null and !persistStatus(self.ctx, status.?)))
            {
                self.persist_failed = true;
                return false;
            }
        }
        return !self.state.coordinator.cancelRequested() and !self.state.stop.requested();
    }
};

fn executeDownload(app: *r4os.App, state: *Runtime, work: *const core.Work) core.Completion {
    const ctx = app.system();
    const files = app.files() orelse return downloadFailure(&ctx, state, contract.result_download_failed, "files-unavailable", false);
    if (!state.download.valid or !state.download.record.valid() or state.download.record.job_id != work.job_id)
        return downloadFailure(&ctx, state, contract.result_selection_stale, "download-snapshot-missing", false);
    const paths = buildDownloadPaths(&state.download.record) orelse
        return downloadFailure(&ctx, state, contract.result_invalid, "download-path-invalid", false);

    var final_info: r4os.abi.FileInfo = .{};
    const final_lookup = ctx.fileInfoRaw(paths.final.asZ().ptr, &final_info);
    if (final_lookup < 0) return downloadFailure(&ctx, state, contract.result_download_failed, "final-info-failed", false);
    if (final_lookup > 0 and final_info.exists != 0) {
        if (final_info.is_dir == 0 and final_info.size == state.download.record.expected_size and
            update_download.verifyReader(&ctx, paths.final.asZ().ptr, state.download.record.expected_size, state.download.record.sha256Text(), state.download.hash_scratch[0..]))
        {
            _ = ctx.fileDelete(paths.part.asZ().ptr);
            return downloadSucceeded(&ctx, state);
        }
        return downloadFailure(&ctx, state, contract.result_publish_failed, "final-conflict", false);
    }

    var part_info: r4os.abi.FileInfo = .{};
    const part_lookup = ctx.fileInfoRaw(paths.part.asZ().ptr, &part_info);
    if (part_lookup < 0) return downloadFailure(&ctx, state, contract.result_download_failed, "partial-info-failed", false);
    const observed_size: ?u64 = if (part_lookup > 0 and part_info.exists != 0 and part_info.is_dir == 0) part_info.size else null;
    var resume_from: u64 = 0;
    switch (update_download.partialAction(state.download.record.expected_size, observed_size)) {
        .start => {
            if (observed_size == null and !createEmptyPart(files, &paths))
                return downloadFailure(&ctx, state, contract.result_download_failed, "partial-create-failed", false);
        },
        .continue_from => |offset| resume_from = offset,
        .verify => resume_from = state.download.record.expected_size,
        .discard => {
            _ = ctx.fileDelete(paths.part.asZ().ptr);
            if (!createEmptyPart(files, &paths))
                return downloadFailure(&ctx, state, contract.result_download_failed, "partial-recreate-failed", false);
        },
    }

    state.download.record.state = .downloading;
    state.download.record.result = contract.result_ok;
    state.download.record.progress = resume_from;
    _ = state.coordinator.progress(work.job_id, resume_from, state.download.record.expected_size);
    updateOfferState(&ctx, &state.download.record, .downloading, contract.result_ok, resume_from);
    if (!persistDownloadRecord(&ctx, &state.download.record))
        return downloadFailure(&ctx, state, contract.result_persist_failed, "download-state-persist-failed", false);

    if (resume_from < state.download.record.expected_size) {
        var web = app.web() orelse return downloadFailure(&ctx, state, contract.result_network_failed, "web-unavailable", false);
        var flow = DownloadFlow{
            .ctx = &ctx,
            .state = state,
            .paths = &paths,
            .job_id = work.job_id,
            .current = resume_from,
            .last_checkpoint = resume_from,
        };
        const result = web.download(
            state.download.record.urlText(),
            state.download.header[0..],
            state.download.io[0..],
            state.download.scratch[0..],
            .{ .context = &flow, .write_fn = DownloadFlow.write },
            .{
                .timeout = timeoutFromMs(state.config.request_timeout_ms),
                .redirect_limit = if (state.config.allow_redirects) 3 else 0,
                .stop = &state.stop.storage,
                .progress = DownloadFlow.progress,
                .progress_context = &flow,
                .headers = "accept:application/octet-stream\n",
                .resume_from = resume_from,
                .expected_size = state.download.record.expected_size,
                .target_authorizer = authorizeDownloadTarget,
                .target_authorization_context = &state.config,
            },
        );
        switch (result) {
            .response => |response| {
                // WebTransport may resume one interrupted response internally.
                // Its final wire response is then 206 even when this operation
                // started at offset zero; each attempt has already passed the
                // strict Range/Content-Range validation in app_web.
                if ((response.status != 200 and response.status != 206) or
                    response.total_size != state.download.record.expected_size or flow.current != state.download.record.expected_size)
                {
                    _ = ctx.fileDelete(paths.part.asZ().ptr);
                    state.download.record.progress = 0;
                    return downloadFailure(&ctx, state, contract.result_partial_discarded, "download-size-mismatch", false);
                }
            },
            .range_not_satisfiable => {
                _ = ctx.fileDelete(paths.part.asZ().ptr);
                state.download.record.progress = 0;
                return downloadFailure(&ctx, state, contract.result_partial_discarded, "download-range-mismatch", false);
            },
            .failure => |failure| {
                if (flow.persist_failed)
                    return downloadFailure(&ctx, state, contract.result_persist_failed, "download-state-persist-failed", false);
                if (failure == .cancelled or state.coordinator.cancelRequested() or state.stop.requested())
                    return downloadFailure(&ctx, state, contract.result_cancelled, "cancelled", false);
                if (failure == .content_length_required or failure == .content_range_required or failure == .content_range_mismatch) {
                    state.download.record.progress = 0;
                    return downloadFailure(&ctx, state, contract.result_partial_discarded, "download-object-mismatch", true);
                }
                return downloadFailure(&ctx, state, contract.result_network_failed, "download-network-failed", false);
            },
        }
    }

    var complete_info: r4os.abi.FileInfo = .{};
    if (ctx.fileInfoRaw(paths.part.asZ().ptr, &complete_info) <= 0 or complete_info.exists == 0 or
        complete_info.is_dir != 0 or complete_info.size != state.download.record.expected_size)
    {
        _ = ctx.fileDelete(paths.part.asZ().ptr);
        state.download.record.progress = 0;
        return downloadFailure(&ctx, state, contract.result_integrity_failed, "download-size-invalid", false);
    }
    if (!update_download.verifyReader(&ctx, paths.part.asZ().ptr, state.download.record.expected_size, state.download.record.sha256Text(), state.download.hash_scratch[0..])) {
        _ = ctx.fileDelete(paths.part.asZ().ptr);
        state.download.record.progress = 0;
        return downloadFailure(&ctx, state, contract.result_integrity_failed, "download-sha256-invalid", false);
    }

    _ = ctx.fileDelete(paths.backup.asZ().ptr);
    const published = files.replaceAtomic(
        paths.final.asZ(),
        paths.part.asZ(),
        paths.backup.asZ(),
        .{ .consume_stage = true, .require_target_absent = true },
    );
    switch (published) {
        .ok => {},
        else => return downloadFailure(&ctx, state, contract.result_publish_failed, "download-publish-failed", false),
    }
    if (!update_download.verifyReader(&ctx, paths.final.asZ().ptr, state.download.record.expected_size, state.download.record.sha256Text(), state.download.hash_scratch[0..]))
        return downloadFailure(&ctx, state, contract.result_integrity_failed, "published-package-invalid", false);
    return downloadSucceeded(&ctx, state);
}

fn createEmptyPart(files: r4os.Files, paths: *const DownloadPaths) bool {
    var writer = switch (files.streamWriter(paths.part.asZ(), r4os.abi.file_stream_open_replace)) {
        .writer => |value| value,
        .failure => return false,
    };
    return switch (writer.finish()) {
        .ok => true,
        else => false,
    };
}

fn downloadSucceeded(ctx: *const r4os.r4sys.Context, state: *Runtime) core.Completion {
    state.download.record.state = .downloaded;
    state.download.record.result = contract.result_ok;
    state.download.record.progress = state.download.record.expected_size;
    updateOfferState(ctx, &state.download.record, .downloaded, contract.result_ok, state.download.record.expected_size);
    const persisted = persistDownloadRecord(ctx, &state.download.record);
    return .{
        .state = .downloaded,
        .result = if (persisted) contract.result_ok else contract.result_persist_failed,
        .progress_current = state.download.record.expected_size,
        .progress_total = state.download.record.expected_size,
        .package_count = 1,
        .completed_count = 1,
        .reason = if (persisted) "downloaded" else "downloaded-state-persist-failed",
    };
}

fn downloadFailure(
    ctx: *const r4os.r4sys.Context,
    state: *Runtime,
    result: i32,
    reason: []const u8,
    discard_partial: bool,
) core.Completion {
    if (state.download.valid) {
        if (discard_partial) discardPart(ctx, &state.download.record);
        state.download.record.state = .failed;
        state.download.record.result = result;
        _ = persistDownloadRecord(ctx, &state.download.record);
        updateOfferState(ctx, &state.download.record, .failed, result, state.download.record.progress);
    }
    return .{
        .state = if (result == contract.result_cancelled) .cancelled else .failed,
        .result = result,
        .progress_current = if (state.download.valid) state.download.record.progress else 0,
        .progress_total = if (state.download.valid) state.download.record.expected_size else 0,
        .package_count = if (state.download.valid) 1 else 0,
        .reason = reason,
    };
}

fn buildDownloadPaths(record: *const update_download.Record) ?DownloadPaths {
    return buildDownloadPathsForFilename(record.filenameText());
}

fn buildDownloadPathsForFilename(filename: []const u8) ?DownloadPaths {
    var buffer: [r4os.path.file_path_max]u8 = undefined;
    var length: usize = 0;
    if (!appendPath(buffer[0..], &length, inbox_prefix) or !appendPath(buffer[0..], &length, filename)) return null;
    const final = r4os.AbsoluteFilePath.parse(buffer[0..length]) catch return null;
    var private_name: [12]u8 = undefined;
    shortDownloadName(filename, "TMP", &private_name);
    length = 0;
    if (!appendPath(buffer[0..], &length, inbox_prefix) or !appendPath(buffer[0..], &length, private_name[0..])) return null;
    const part = r4os.AbsoluteFilePath.parse(buffer[0..length]) catch return null;
    shortDownloadName(filename, "BAK", &private_name);
    length = 0;
    if (!appendPath(buffer[0..], &length, inbox_prefix) or !appendPath(buffer[0..], &length, private_name[0..])) return null;
    const backup = r4os.AbsoluteFilePath.parse(buffer[0..length]) catch return null;
    return .{ .part = part, .final = final, .backup = backup };
}

fn shortDownloadName(filename: []const u8, extension: *const [3]u8, out: *[12]u8) void {
    var hash: u32 = 2_166_136_261;
    for (filename) |byte| {
        hash ^= byte;
        hash *%= 16_777_619;
    }
    const hex = "0123456789ABCDEF";
    for (0..8) |index| {
        const shift: u5 = @intCast((7 - index) * 4);
        out[index] = hex[@as(usize, @intCast((hash >> shift) & 0xF))];
    }
    out[8] = '.';
    @memcpy(out[9..12], extension);
}

fn hydrateDownloadedOffers(ctx: *const r4os.r4sys.Context, state: *Runtime) void {
    var result_index: usize = 0;
    while (result_index < state.results.plan.package_count) : (result_index += 1) {
        const package_index = state.results.plan.packageIndex(result_index) orelse continue;
        if (package_index >= state.results.release.package_count) continue;
        const package = &state.results.release.packages[package_index];
        const paths = buildDownloadPathsForFilename(package.filename) orelse continue;
        var info: r4os.abi.FileInfo = .{};
        const lookup = ctx.fileInfoRaw(paths.final.asZ().ptr, &info);
        if (lookup <= 0 or info.exists == 0 or info.is_dir != 0) continue;
        if (info.size == package.size and update_download.verifyReader(
            ctx,
            paths.final.asZ().ptr,
            package.size,
            package.sha256,
            state.download.hash_scratch[0..],
        )) {
            state.results.offer_states[result_index] = @intFromEnum(contract.State.downloaded);
            state.results.offer_results[result_index] = contract.result_ok;
            state.results.offer_progress[result_index] = package.size;
        } else {
            state.results.offer_states[result_index] = @intFromEnum(contract.State.failed);
            state.results.offer_results[result_index] = contract.result_integrity_failed;
            state.results.offer_progress[result_index] = 0;
        }
    }
}

fn appendPath(out: []u8, length: *usize, value: []const u8) bool {
    if (value.len > out.len - length.*) return false;
    @memcpy(out[length.* .. length.* + value.len], value);
    length.* += value.len;
    return true;
}

fn discardPart(ctx: *const r4os.r4sys.Context, record: *const update_download.Record) void {
    const paths = buildDownloadPaths(record) orelse return;
    _ = ctx.fileDelete(paths.part.asZ().ptr);
}

fn authorizeDownloadTarget(raw: ?*anyopaque, target: []const u8) bool {
    const config: *const ServiceConfig = @ptrCast(@alignCast(raw orelse return false));
    return sameHttpsOrigin(config.catalog_url[0..config.catalog_url_len], target);
}

fn sameHttpsOrigin(base: []const u8, target: []const u8) bool {
    const prefix = "https://";
    if (!startsWithIgnoreCase(base, prefix) or !startsWithIgnoreCase(target, prefix)) return false;
    const base_rest = base[prefix.len..];
    const target_rest = target[prefix.len..];
    const base_end = std.mem.indexOfScalar(u8, base_rest, '/') orelse base_rest.len;
    const target_end = std.mem.indexOfScalar(u8, target_rest, '/') orelse target_rest.len;
    return std.ascii.eqlIgnoreCase(base_rest[0..base_end], target_rest[0..target_end]);
}

fn updateOfferState(
    ctx: *const r4os.r4sys.Context,
    record: *const update_download.Record,
    state: update_download.State,
    result: i32,
    progress: u64,
) void {
    var attempt: u32 = 0;
    while (attempt < 100) : (attempt += 1) {
        if (tryAcquireResultsLock()) {
            defer releaseResultsLock();
            if (runtime.results.job_id == record.search_job_id and record.result_index < runtime.results.plan.package_count) {
                runtime.results.offer_states[record.result_index] = switch (state) {
                    .available => @intFromEnum(contract.State.available),
                    .downloading => @intFromEnum(contract.State.downloading),
                    .downloaded => @intFromEnum(contract.State.downloaded),
                    .failed => @intFromEnum(contract.State.failed),
                };
                runtime.results.offer_results[record.result_index] = result;
                runtime.results.offer_progress[record.result_index] = progress;
            }
            return;
        }
        ctx.sleepTicks(1);
    }
}

fn setOfferContractState(
    ctx: *const r4os.r4sys.Context,
    search_job_id: u32,
    result_index: usize,
    state: contract.State,
    result: i32,
    progress: u64,
) void {
    var attempt: u32 = 0;
    while (attempt < 100) : (attempt += 1) {
        if (tryAcquireResultsLock()) {
            defer releaseResultsLock();
            if (runtime.results.job_id == search_job_id and result_index < runtime.results.plan.package_count) {
                runtime.results.offer_states[result_index] = @intFromEnum(state);
                runtime.results.offer_results[result_index] = result;
                runtime.results.offer_progress[result_index] = progress;
            }
            return;
        }
        ctx.sleepTicks(1);
    }
}

const LiveReleaseBinding = struct {
    search_job_id: u32,
    result_index: usize,
    source_len: u16,
    target_len: u16,
    source: [contract.offer_release_capacity]u8 = .{0} ** contract.offer_release_capacity,
    target: [contract.offer_release_capacity]u8 = .{0} ** contract.offer_release_capacity,

    fn sourceText(self: *const LiveReleaseBinding) []const u8 {
        return self.source[0..self.source_len];
    }

    fn targetText(self: *const LiveReleaseBinding) []const u8 {
        return self.target[0..self.target_len];
    }
};

/// Binds an individual install to the immutable offer that produced its
/// downloaded inbox file. A later search may reuse that file only when every
/// immutable catalog field still matches; in that case the binding moves to
/// the new snapshot/result row. Administrative installs without a current
/// search snapshot remain valid, but can never advance the central release.
fn selectedLiveReleaseBinding(
    ctx: *const r4os.r4sys.Context,
    state: *Runtime,
    selected_path: []const u8,
) ?LiveReleaseBinding {
    if (!state.download.valid or !state.download.record.valid() or state.download.record.state != .downloaded)
        return null;
    const selected = r4os.AbsoluteFilePath.parse(selected_path) catch return null;
    const expected = buildDownloadPaths(&state.download.record) orelse return null;
    if (!std.ascii.eqlIgnoreCase(selected.bytes(), expected.final.bytes())) return null;

    var attempt: u32 = 0;
    while (!tryAcquireResultsLock()) : (attempt += 1) {
        if (attempt >= 100) return null;
        ctx.sleepTicks(1);
    }
    defer releaseResultsLock();

    const record = &state.download.record;
    if (state.results.job_id == 0 or state.results.plan.package_count == 0) return null;
    var matched_result: ?usize = null;
    var candidate: usize = 0;
    while (candidate < state.results.plan.package_count) : (candidate += 1) {
        const package_index = state.results.plan.packageIndex(candidate) orelse return null;
        if (package_index >= state.results.release.package_count) return null;
        const package = &state.results.release.packages[package_index];
        if (package.size != record.expected_size or
            !std.mem.eql(u8, package.id, record.packageIdText()) or
            !std.mem.eql(u8, package.package_version, record.packageVersionText()) or
            !std.mem.eql(u8, state.results.release.release, record.releaseText()) or
            !std.ascii.eqlIgnoreCase(package.filename, record.filenameText()) or
            !std.ascii.eqlIgnoreCase(package.sha256, record.sha256Text()) or
            !std.mem.eql(u8, package.download_url, record.urlText())) continue;
        const current_paths = buildDownloadPathsForFilename(package.filename) orelse return null;
        if (!std.ascii.eqlIgnoreCase(selected.bytes(), current_paths.final.bytes())) continue;
        if (matched_result != null) return null;
        matched_result = candidate;
    }
    const result_index = matched_result orelse return null;

    const source = state.results.current_release[0..state.results.current_release_len];
    const target = state.results.release.release;
    if (source.len == 0 or source.len > contract.offer_release_capacity or
        target.len == 0 or target.len > contract.offer_release_capacity) return null;
    var binding = LiveReleaseBinding{
        .search_job_id = state.results.job_id,
        .result_index = result_index,
        .source_len = @intCast(source.len),
        .target_len = @intCast(target.len),
    };
    @memcpy(binding.source[0..source.len], source);
    @memcpy(binding.target[0..target.len], target);
    return binding;
}

fn executeInstall(app: *r4os.App, state: *Runtime, work: *const core.Work) core.Completion {
    const ctx = app.system();
    const path = work.request.selectionText() orelse return invalidCompletion("invalid-path");
    if (path.len == 0 or path.len > r4os.path.file_path_max) return invalidCompletion("invalid-path");
    if (state.coordinator.cancelRequested() or state.stop.requested()) return cancelledCompletion();

    const release_binding = selectedLiveReleaseBinding(&ctx, state, path);

    const transitioned = if (release_binding) |binding|
        state.coordinator.transitionPackage(
            work.job_id,
            .installing,
            binding.search_job_id,
            @intCast(binding.result_index),
            1,
            0,
            "installing",
        )
    else
        state.coordinator.transition(work.job_id, .installing, "installing");
    if (transitioned) {
        if (state.coordinator.snapshot()) |status| _ = persistStatus(&ctx, status);
    }

    var engine = update_engine.Engine.init(app);
    const result = engine.stage(path);
    if (state.coordinator.cancelRequested() or state.stop.requested()) {
        // The engine operation is atomic/journaled. Cancellation after entry
        // never pretends that an already durable stage was undone.
        if (result.exit_code != 0) return cancelledCompletion();
    }
    var completion = completionFromEngine(&result);
    const binding = release_binding orelse return completion;
    setOfferContractState(
        &ctx,
        binding.search_job_id,
        binding.result_index,
        completion.state,
        completion.result,
        0,
    );
    if (result.exit_code != 0) return completion;
    if (completion.state == .staged or completion.state == .pending_restart) {
        if (!snapshotPreparedForRestart(&ctx, state, &binding)) return completion;
        state.prepared_restart = true;
        state.prepared_search_job_id = binding.search_job_id;
        completion.state = .pending_restart;
        completion.flags |= contract.flag_restart_required | contract.flag_results_ready;
        completion.reason = "restart-ready";
        setOfferContractState(
            &ctx,
            binding.search_job_id,
            binding.result_index,
            .pending_restart,
            contract.result_ok,
            0,
        );
        return completion;
    }
    if (completion.state != .installed or !targetReleaseReached(app, state)) return completion;

    const confirmed = engine.confirmLiveRelease(binding.sourceText(), binding.targetText());
    if (confirmed.exit_code != 0) return completionFromEngine(&confirmed);
    completion.reason = "release-installed";
    return completion;
}

fn executeUpdateAll(app: *r4os.App, state: *Runtime, work: *const core.Work) core.Completion {
    const ctx = app.system();
    if (work.source_job_id == 0 or state.results.job_id != work.source_job_id or state.results.plan.package_count == 0)
        return invalidCompletion("update-all-snapshot-stale");
    const package_count: u32 = @intCast(state.results.plan.package_count);
    var completed: u32 = 0;
    state.prepared_restart = false;
    state.prepared_search_job_id = 0;

    // Phase one is intentionally complete before any package is applied.
    // Every R4U is downloaded and transport-verified against its catalog hash.
    var result_index: usize = 0;
    while (result_index < state.results.plan.package_count) : (result_index += 1) {
        if (state.coordinator.cancelRequested() or state.stop.requested()) return cancelledCompletion();
        const current_state = contract.stateFromWire(state.results.offer_states[result_index]) orelse .failed;
        if (current_state == .installed or current_state == .staged or current_state == .pending_restart) continue;
        const record = downloadRecordForResult(work.job_id, work.source_job_id, result_index) orelse
            return updateAllFailure(package_count, completed, contract.result_invalid, "download-record-invalid");
        state.download.record = record;
        state.download.valid = true;
        _ = state.coordinator.transitionPackage(
            work.job_id,
            .downloading,
            work.source_job_id,
            @intCast(result_index),
            package_count,
            completed,
            "update-all-download",
        );
        const downloaded = executeDownload(app, state, work);
        if (downloaded.result != contract.result_ok)
            return updateAllFailure(package_count, completed, downloaded.result, downloaded.reason);
    }

    // Phase two verifies every immutable R4U through the system update engine
    // before the first live target can change.
    var engine = update_engine.Engine.init(app);
    result_index = 0;
    while (result_index < state.results.plan.package_count) : (result_index += 1) {
        if (state.coordinator.cancelRequested() or state.stop.requested()) return cancelledCompletion();
        const current_state = contract.stateFromWire(state.results.offer_states[result_index]) orelse .failed;
        if (current_state == .installed or current_state == .staged or current_state == .pending_restart) continue;
        const path = packageInboxPath(result_index) orelse
            return updateAllFailure(package_count, completed, contract.result_invalid, "package-path-invalid");
        _ = state.coordinator.transitionPackage(
            work.job_id,
            .verifying,
            work.source_job_id,
            @intCast(result_index),
            package_count,
            completed,
            "update-all-verify",
        );
        setOfferContractState(&ctx, work.source_job_id, result_index, .verifying, contract.result_ok, 0);
        const verified = engine.verifyForBatch(path.bytes());
        if (verified.exit_code != 0) {
            setOfferContractState(&ctx, work.source_job_id, result_index, .failed, verified.exit_code, 0);
            return updateAllFailure(package_count, completed, verified.exit_code, verified.reasonText());
        }
        const package_index = state.results.plan.packageIndex(result_index) orelse
            return updateAllFailure(package_count, completed, contract.result_invalid, "package-index-invalid");
        setOfferContractState(
            &ctx,
            work.source_job_id,
            result_index,
            .downloaded,
            contract.result_ok,
            state.results.release.packages[package_index].size,
        );
    }

    // Phase three keeps the server-provided, client-validated topological
    // order. Engine.stage applies only safe live R4X packages and otherwise
    // appends the package to one durable restart batch.
    var restart_required = false;
    result_index = 0;
    while (result_index < state.results.plan.package_count) : (result_index += 1) {
        if (state.coordinator.cancelRequested() or state.stop.requested()) return cancelledCompletion();
        const current_state = contract.stateFromWire(state.results.offer_states[result_index]) orelse .failed;
        if (current_state == .installed) {
            completed += 1;
            continue;
        }
        if (current_state == .staged or current_state == .pending_restart) {
            completed += 1;
            restart_required = true;
            continue;
        }
        const path = packageInboxPath(result_index) orelse
            return updateAllFailure(package_count, completed, contract.result_invalid, "package-path-invalid");
        _ = state.coordinator.transitionPackage(
            work.job_id,
            .installing,
            work.source_job_id,
            @intCast(result_index),
            package_count,
            completed,
            "update-all-install",
        );
        setOfferContractState(&ctx, work.source_job_id, result_index, .installing, contract.result_ok, 0);
        const installed = engine.stage(path.bytes());
        if (installed.exit_code != 0) {
            setOfferContractState(&ctx, work.source_job_id, result_index, .failed, installed.exit_code, 0);
            return updateAllFailure(package_count, completed, installed.exit_code, installed.reasonText());
        }
        const offer_state: contract.State = switch (installed.state) {
            .staged => .staged,
            .pending_restart, .restart_required => .pending_restart,
            else => .installed,
        };
        setOfferContractState(&ctx, work.source_job_id, result_index, offer_state, contract.result_ok, 0);
        completed += 1;
        restart_required = restart_required or offer_state == .staged or offer_state == .pending_restart or installed.restart_required;
    }

    if (restart_required) {
        state.prepared_restart = true;
        state.prepared_search_job_id = work.source_job_id;
        return .{
            .state = .pending_restart,
            .result = contract.result_ok,
            .flags = contract.flag_restart_required | contract.flag_results_ready,
            .package_count = package_count,
            .completed_count = completed,
            .reason = "restart-ready",
        };
    }

    if (!targetReleaseReached(app, state))
        return updateAllFailure(package_count, completed, contract.result_requirement_unmet, "target-release-incomplete");
    const source_release = state.results.current_release[0..state.results.current_release_len];
    const confirmed = engine.confirmLiveRelease(source_release, state.results.release.release);
    if (confirmed.exit_code != 0)
        return updateAllFailure(package_count, completed, confirmed.exit_code, confirmed.reasonText());
    return .{
        .state = .installed,
        .result = contract.result_ok,
        .flags = contract.flag_results_ready,
        .package_count = package_count,
        .completed_count = completed,
        .reason = "release-installed",
    };
}

fn executeRestart(app: *r4os.App, state: *Runtime, work: *const core.Work) core.Completion {
    if (!state.prepared_restart or state.prepared_search_job_id != work.source_job_id)
        return invalidCompletion("restart-batch-not-ready");
    var engine = update_engine.Engine.init(app);
    const result = engine.commitBatch();
    // Successful commit calls systemReboot and does not normally return to
    // this worker. Any returned result is still represented faithfully.
    if (result.exit_code != 0) return completionFromEngine(&result);
    return .{
        .state = .pending_restart,
        .result = contract.result_ok,
        .flags = contract.flag_restart_required,
        .package_count = result.batch_package_count,
        .completed_count = result.committed_count,
        .reason = "restarting",
    };
}

fn updateAllFailure(package_count: u32, completed_count: u32, result: i32, reason: []const u8) core.Completion {
    return .{
        .state = if (result == contract.result_cancelled) .cancelled else .failed,
        .result = result,
        .package_count = package_count,
        .completed_count = completed_count,
        .reason = if (reason.len != 0) reason else "update-all-failed",
    };
}

fn downloadRecordForResult(job_id: u32, search_job_id: u32, result_index: usize) ?update_download.Record {
    var offer = contract.Offer{};
    if (!fillOffer(&offer, result_index)) return null;
    return update_download.Record.init(
        job_id,
        search_job_id,
        @intCast(result_index),
        offer.package_id[0..offer.package_id_len],
        offer.package_version[0..offer.package_version_len],
        offer.release[0..offer.release_len],
        offer.filename[0..offer.filename_len],
        offer.sha256[0..offer.sha256_len],
        offer.download_url[0..offer.download_url_len],
        offer.size_bytes,
    );
}

fn packageInboxPath(result_index: usize) ?r4os.AbsoluteFilePath {
    const package_index = runtime.results.plan.packageIndex(result_index) orelse return null;
    if (package_index >= runtime.results.release.package_count) return null;
    return (buildDownloadPathsForFilename(runtime.results.release.packages[package_index].filename) orelse return null).final;
}

fn targetReleaseReached(app: *r4os.App, state: *Runtime) bool {
    const ctx = app.system();
    var lock_attempt: u32 = 0;
    while (!tryAcquireResultsLock()) : (lock_attempt += 1) {
        if (lock_attempt >= 100) return false;
        ctx.sleepTicks(1);
    }
    defer releaseResultsLock();
    const inventory_read = ctx.fileRead("C:\\R4OS\\CONFIG\\MODULES.JSON", state.results.inventory_bytes[0..]);
    if (inventory_read <= 0 or inventory_read > @as(i32, @intCast(state.results.inventory_bytes.len))) return false;
    state.results.inventory_len = @intCast(inventory_read);
    if (!r4os.system_update_inventory.Inventory.parse(
        state.results.inventory_bytes[0..state.results.inventory_len],
        &state.results.inventory,
    )) return false;
    const active = state.results.active_kernel[0..state.results.active_kernel_len];
    const remaining = update_catalog.buildPlan(
        &state.results.release,
        &state.results.inventory,
        state.results.release.release,
        if (active.len == 0) null else active,
    ) catch return false;
    return remaining.package_count == 0 and !remaining.restart_required;
}

/// An individual restart package may expose the commit button only after
/// every package in the immutable search plan is either live-installed or
/// durably present in the same engine-enforced restart batch.
fn snapshotPreparedForRestart(
    ctx: *const r4os.r4sys.Context,
    state: *const Runtime,
    binding: *const LiveReleaseBinding,
) bool {
    var attempt: u32 = 0;
    while (!tryAcquireResultsLock()) : (attempt += 1) {
        if (attempt >= 100) return false;
        ctx.sleepTicks(1);
    }
    defer releaseResultsLock();
    if (state.results.job_id != binding.search_job_id or state.results.plan.package_count == 0)
        return false;
    var index: usize = 0;
    while (index < state.results.plan.package_count) : (index += 1) {
        switch (contract.stateFromWire(state.results.offer_states[index]) orelse return false) {
            .installed, .staged, .pending_restart => {},
            else => return false,
        }
    }
    return true;
}

fn completionFromEngine(result: *const update_engine.Result) core.Completion {
    if (result.exit_code != 0) {
        return .{
            .state = if (result.state == .busy) .failed else .failed,
            .result = result.exit_code,
            .flags = if (result.restart_required) contract.flag_restart_required else 0,
            .package_count = result.batch_package_count,
            .completed_count = result.committed_count,
            .reason = result.reasonText(),
        };
    }
    const target_state: contract.State = switch (result.state) {
        .installed => .installed,
        .staged => .staged,
        .pending_restart, .restart_required => .pending_restart,
        else => .installed,
    };
    return .{
        .state = target_state,
        .result = result.exit_code,
        .flags = if (result.restart_required or target_state == .pending_restart) contract.flag_restart_required else 0,
        .package_count = if (result.batch_package_count != 0) result.batch_package_count else 1,
        .completed_count = result.committed_count,
        .reason = if (result.reasonText().len != 0) result.reasonText() else contract.stateName(@intFromEnum(target_state)),
    };
}

fn cancelledCompletion() core.Completion {
    return .{ .state = .cancelled, .result = contract.result_cancelled, .reason = "cancelled" };
}

fn invalidCompletion(reason: []const u8) core.Completion {
    return .{ .state = .failed, .result = contract.result_invalid, .reason = reason };
}

fn loadConfig(ctx: *const r4os.r4sys.Context) ServiceConfig {
    var bytes: [config_capacity]u8 = undefined;
    const got = ctx.fileRead(contract.config_path, bytes[0..]);
    if (got <= 0 or got > @as(i32, @intCast(bytes.len))) return .{};
    const document = r4std.settings.Document.init(bytes[0..@intCast(got)]);
    if (!document.hasSupportedFormat()) return .{};
    const schema = document.schemaName() orelse return .{};
    if (!textEquals(schema, "R4OS_UPDATE_CLIENT")) return .{};

    var config = ServiceConfig{};
    const server = document.value("SERVER") orelse return config;
    const catalog_url = document.value("CATALOG_URL") orelse return config;
    const key = document.value("MAGIC_KEY") orelse return config;
    const channel = document.value("CHANNEL") orelse return config;
    const auth_port = document.u32Value("AUTH_PORT") orelse return config;
    const update_port = document.u32Value("UPDATE_PORT") orelse return config;
    const parsed_catalog = switch (r4os.http.parseUrl(catalog_url)) {
        .value => |value| value,
        .failure => return config,
    };
    if (!validServerHost(server) or !validHttpsUrl(catalog_url) or parsed_catalog.scheme != .https or
        !std.ascii.eqlIgnoreCase(server, parsed_catalog.host) or !validMagicKey(key) or channel.len == 0 or channel.len > config.channel.len or
        auth_port == 0 or auth_port > 65535 or update_port == 0 or update_port > 65535)
        return config;

    config.server_len = copyFixed(config.server[0..], server);
    config.catalog_url_len = copyFixed(config.catalog_url[0..], catalog_url);
    config.magic_key_len = copyFixed(config.magic_key[0..], key);
    config.channel_len = copyFixed(config.channel[0..], channel);
    config.auth_port = @intCast(auth_port);
    config.update_port = @intCast(update_port);
    config.enabled = document.boolValue("ENABLED") orelse true;
    config.connect_timeout_ms = document.u32Value("CONNECT_TIMEOUT_MS") orelse 5000;
    config.request_timeout_ms = document.u32Value("REQUEST_TIMEOUT_MS") orelse 30000;
    config.allow_redirects = document.boolValue("ALLOW_REDIRECTS") orelse false;
    config.valid = config.enabled and config.connect_timeout_ms > 0 and config.request_timeout_ms > 0;
    return config;
}

fn loadStatus(ctx: *const r4os.r4sys.Context) ?contract.Status {
    var bytes: [state_capacity]u8 = undefined;
    const got = ctx.fileRead(contract.state_path, bytes[0..]);
    if (got <= 0 or got > @as(i32, @intCast(bytes.len))) return null;
    const document = r4std.settings.Document.init(bytes[0..@intCast(got)]);
    if (!document.hasSupportedFormat()) return null;
    const schema = document.schemaName() orelse return null;
    if (!textEquals(schema, "UPDSVC_STATE")) return null;
    var status = contract.Status{};
    status.job_id = document.u32Value("JOB_ID") orelse return null;
    status.generation = document.u32Value("GENERATION") orelse return null;
    const operation = document.u32Value("OPERATION") orelse return null;
    const state_value = document.u32Value("STATE") orelse return null;
    if (operation > 65535 or state_value > 65535) return null;
    status.operation = @intCast(operation);
    status.state = @intCast(state_value);
    status.flags = document.u32Value("FLAGS") orelse 0;
    status.result = document.i32Value("RESULT") orelse contract.result_not_ready;
    status.progress_current = document.u32Value("PROGRESS_CURRENT") orelse 0;
    status.progress_total = document.u32Value("PROGRESS_TOTAL") orelse 0;
    status.package_count = document.u32Value("PACKAGE_COUNT") orelse 0;
    status.completed_count = document.u32Value("COMPLETED_COUNT") orelse 0;
    status.source_job_id = document.u32Value("SOURCE_JOB_ID") orelse 0;
    status.result_index = document.u32Value("RESULT_INDEX") orelse 0;
    contract.setReason(&status, document.value("REASON") orelse "recovered");
    return if (status.valid()) status else null;
}

fn persistStatus(ctx: *const r4os.r4sys.Context, status: contract.Status) bool {
    acquirePersistLock(ctx);
    defer releasePersistLock();
    var bytes: [state_capacity]u8 = undefined;
    var writer = r4std.settings.Writer.init(bytes[0..]);
    writer.writeHeader("UPDSVC_STATE");
    writer.writePairU32("JOB_ID", status.job_id);
    writer.writePairU32("GENERATION", status.generation);
    writer.writePairU32("OPERATION", status.operation);
    writer.writePairU32("STATE", status.state);
    writer.writePairU32("FLAGS", status.flags);
    writer.writePairI32("RESULT", status.result);
    if (status.progress_current > std.math.maxInt(u32) or status.progress_total > std.math.maxInt(u32)) return false;
    writer.writePairU32("PROGRESS_CURRENT", @intCast(status.progress_current));
    writer.writePairU32("PROGRESS_TOTAL", @intCast(status.progress_total));
    writer.writePairU32("PACKAGE_COUNT", status.package_count);
    writer.writePairU32("COMPLETED_COUNT", status.completed_count);
    writer.writePairU32("SOURCE_JOB_ID", status.source_job_id);
    writer.writePairU32("RESULT_INDEX", status.result_index);
    writer.writePair("REASON", status.reasonText());
    if (!writer.ok()) return false;
    return r4std.config.saveDocument(ctx, contract.state_path, writer.bytes()) >= 0;
}

fn loadDownloadRecord(ctx: *const r4os.r4sys.Context, out: *update_download.Record) bool {
    var bytes: [update_download.journal_capacity]u8 = undefined;
    const got = ctx.fileRead(contract.download_state_path, bytes[0..]);
    if (got <= 0 or got > @as(i32, @intCast(bytes.len))) return false;
    return update_download.parse(bytes[0..@intCast(got)], out);
}

fn persistDownloadRecord(ctx: *const r4os.r4sys.Context, record: *const update_download.Record) bool {
    var bytes: [update_download.journal_capacity]u8 = undefined;
    const encoded = update_download.encode(record, bytes[0..]) orelse return false;
    acquirePersistLock(ctx);
    defer releasePersistLock();
    return r4std.config.saveDocument(ctx, contract.download_state_path, encoded) >= 0;
}

fn acquirePersistLock(ctx: *const r4os.r4sys.Context) void {
    while (@cmpxchgStrong(u32, &runtime.persist_lock, 0, 1, .acquire, .monotonic) != null)
        ctx.sleepTicks(1);
}

fn releasePersistLock() void {
    @atomicStore(u32, &runtime.persist_lock, 0, .release);
}

fn snapshotBlocking(ctx: *const r4os.r4sys.Context) ?contract.Status {
    var attempt: u32 = 0;
    while (attempt < 100) : (attempt += 1) {
        if (runtime.coordinator.snapshot()) |status| return status;
        ctx.sleepTicks(1);
    }
    return null;
}

fn markDurableBlocking(ctx: *const r4os.r4sys.Context, job_id: u32, persisted: bool) bool {
    var attempt: u32 = 0;
    while (attempt < 100) : (attempt += 1) {
        if (runtime.coordinator.markDurable(job_id, persisted)) return true;
        if (!persisted) return false;
        ctx.sleepTicks(1);
    }
    return false;
}

fn decodeStruct(comptime T: type, bytes: []const u8) ?T {
    if (bytes.len != @sizeOf(T)) return null;
    var value: T = undefined;
    const out: [*]u8 = @ptrCast(&value);
    @memcpy(out[0..@sizeOf(T)], bytes);
    return value;
}

fn validMagicKey(value: []const u8) bool {
    if (value.len != 34 or value[0] != '0' or (value[1] != 'x' and value[1] != 'X')) return false;
    for (value[2..]) |ch| if (!isHex(ch)) return false;
    return true;
}

fn isHex(ch: u8) bool {
    return (ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'f') or (ch >= 'A' and ch <= 'F');
}

fn validIpv4(value: []const u8) bool {
    var parts: u8 = 0;
    var digits: u8 = 0;
    var number: u16 = 0;
    for (value) |ch| {
        if (ch >= '0' and ch <= '9') {
            digits += 1;
            if (digits > 3) return false;
            number = number * 10 + (ch - '0');
            if (number > 255) return false;
        } else if (ch == '.' and digits != 0 and parts < 3) {
            parts += 1;
            digits = 0;
            number = 0;
        } else return false;
    }
    return parts == 3 and digits != 0;
}

fn validServerHost(value: []const u8) bool {
    if (validIpv4(value)) return true;
    if (value.len == 0 or value.len > 63 or value[0] == '.' or value[value.len - 1] == '.') return false;
    var label_len: usize = 0;
    for (value) |byte| {
        if (byte == '.') {
            if (label_len == 0) return false;
            label_len = 0;
        } else if ((byte >= 'a' and byte <= 'z') or (byte >= 'A' and byte <= 'Z') or (byte >= '0' and byte <= '9') or byte == '-') {
            label_len += 1;
            if (label_len > 63) return false;
        } else return false;
    }
    return label_len != 0;
}

fn parseIpv4(value: []const u8) ?r4os.Ipv4Address {
    if (!validIpv4(value)) return null;
    var octets: [4]u8 = undefined;
    var index: usize = 0;
    var number: u16 = 0;
    for (value) |byte| {
        if (byte == '.') {
            octets[index] = @intCast(number);
            index += 1;
            number = 0;
        } else {
            number = number * 10 + (byte - '0');
        }
    }
    octets[index] = @intCast(number);
    return r4os.Ipv4Address.fromBytes(octets);
}

fn validHttpsUrl(value: []const u8) bool {
    if (value.len <= "https://".len or value.len > update_catalog.max_download_url_bytes or
        !startsWithIgnoreCase(value, "https://")) return false;
    for (value) |byte| {
        if (byte <= ' ' or byte >= 0x7f or byte == '\\' or byte == '#') return false;
    }
    return true;
}

fn startsWithIgnoreCase(value: []const u8, prefix: []const u8) bool {
    return value.len >= prefix.len and std.ascii.eqlIgnoreCase(value[0..prefix.len], prefix);
}

fn timeoutFromMs(milliseconds: u32) r4os.time_contract.Timeout {
    return r4os.time_contract.timeoutFinite(r4os.time_contract.durationFromNanoseconds(@as(u64, milliseconds) * 1_000_000));
}

fn installedKernel(inventory: *const r4os.system_update_inventory.Inventory) ?[]const u8 {
    for (inventory.entries[0..inventory.count]) |entry| if (entry.kind == .kernel) return entry.version;
    return null;
}

fn tryAcquireResultsLock() bool {
    return @cmpxchgStrong(u32, &runtime.results_lock, 0, 1, .acquire, .monotonic) == null;
}

fn releaseResultsLock() void {
    @atomicStore(u32, &runtime.results_lock, 0, .release);
}

fn fillOffer(out: *contract.Offer, result_index: usize) bool {
    const package_index = runtime.results.plan.packageIndex(result_index) orelse return false;
    if (package_index >= runtime.results.release.package_count) return false;
    const package = &runtime.results.release.packages[package_index];
    const delta = update_catalog.packageDelta(&runtime.results.release, &runtime.results.inventory, package) catch return false;
    out.* = .{
        .flags = (if (package.reboot_required) contract.offer_flag_restart_required else 0) |
            (if (package.priority == .foundation) contract.offer_flag_foundation else 0) |
            (if (delta.repair_count != 0) contract.offer_flag_mandatory_repair else 0),
        .install_order = package.install_order,
        .size_bytes = package.size,
        .progress_current = runtime.results.offer_progress[result_index],
        .progress_total = package.size,
        .result = runtime.results.offer_results[result_index],
        .state = runtime.results.offer_states[result_index],
        .component_count = package.component_count,
        .update_component_count = delta.update_count + delta.repair_count,
    };
    if (!copyOfferText(out.package_id[0..], &out.package_id_len, package.id) or
        !copyOfferText(out.package_version[0..], &out.package_version_len, package.package_version) or
        !copyOfferText(out.release[0..], &out.release_len, runtime.results.release.release) or
        !copyDecodedOfferText(out.title[0..], &out.title_len, package.title) or
        !copyDecodedOfferText(out.description[0..], &out.description_len, package.description) or
        !copyOfferText(out.filename[0..], &out.filename_len, package.filename) or
        !copyOfferText(out.sha256[0..], &out.sha256_len, package.sha256) or
        !copyOfferText(out.download_url[0..], &out.download_url_len, package.download_url)) return false;
    return out.valid();
}

fn fillComponent(out: *contract.OfferComponent, package: *const update_catalog.Package, component_index: usize) bool {
    if (component_index >= package.component_count) return false;
    const components = runtime.results.release.packageComponents(package);
    const component = &components[component_index];
    const installed = update_catalog.installedEntry(
        &runtime.results.inventory,
        component.kind,
        component.name,
        component.target,
    );
    const active_kernel = runtime.results.active_kernel[0..runtime.results.active_kernel_len];
    const installed_version = if (installed) |entry| entry.version else "";
    out.* = .{
        .flags = (if (installed == null) contract.component_flag_missing else 0) |
            (if (component.kind == .kernel) contract.component_flag_kernel else 0) |
            (if (component.install == .restart) contract.component_flag_restart_required else 0) |
            (if (component.kind == .kernel and installed != null and active_kernel.len != 0 and
                !std.mem.eql(u8, installed_version, active_kernel)) contract.component_flag_active_differs else 0),
    };
    if (!copyOfferText(out.name[0..], &out.name_len, component.name) or
        !copyOfferText(out.kind[0..], &out.kind_len, component.kind.text()) or
        !copyOfferText(out.installed_version[0..], &out.installed_version_len, installed_version) or
        !copyOfferText(out.offered_version[0..], &out.offered_version_len, component.version) or
        !copyOfferText(out.active_version[0..], &out.active_version_len, if (component.kind == .kernel) active_kernel else "") or
        !copyOfferText(out.target[0..], &out.target_len, component.target)) return false;
    return out.valid();
}

fn copyOfferText(out: []u8, length: *u16, value: []const u8) bool {
    if (value.len > out.len or value.len > std.math.maxInt(u16)) return false;
    @memset(out, 0);
    if (value.len != 0) @memcpy(out[0..value.len], value);
    length.* = @intCast(value.len);
    return true;
}

fn copyDecodedOfferText(out: []u8, length: *u16, raw: []const u8) bool {
    @memset(out, 0);
    const decoded = update_catalog.decodeJsonString(raw, out) orelse return false;
    if (decoded.len > std.math.maxInt(u16)) return false;
    length.* = @intCast(decoded.len);
    return true;
}

fn copyFixed(out: []u8, value: []const u8) usize {
    @memset(out, 0);
    const len = @min(out.len, value.len);
    if (len != 0) @memcpy(out[0..len], value[0..len]);
    return len;
}

fn textEquals(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (left != right) return false;
    return true;
}

fn hasArg(args: []const u8, wanted: []const u8) bool {
    var offset: usize = 0;
    while (offset < args.len and args[offset] != 0) {
        while (offset < args.len and (args[offset] == ' ' or args[offset] == '\t')) : (offset += 1) {}
        const start = offset;
        while (offset < args.len and args[offset] != 0 and args[offset] != ' ' and args[offset] != '\t') : (offset += 1) {}
        if (textEqualsIgnoreCase(args[start..offset], wanted)) return true;
    }
    return false;
}

fn textEqualsIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| if (upper(left) != upper(right)) return false;
    return true;
}

fn upper(ch: u8) u8 {
    if (ch >= 'a' and ch <= 'z') return ch - ('a' - 'A');
    return ch;
}

fn runSelfTest(app: *r4os.App) i32 {
    var coordinator = core.Coordinator.init(true);
    const accepted = coordinator.submit(contract.op_search, .{});
    const job_id = switch (accepted) {
        .accepted => |ack| ack.job_id,
        else => return selfTestFail(app, "submit"),
    };
    if (!coordinator.markDurable(job_id, true)) return selfTestFail(app, "durable");
    _ = coordinator.takeWork() orelse return selfTestFail(app, "worker");
    if (!coordinator.complete(job_id, .{ .state = .installed, .result = 0, .reason = "ok" }))
        return selfTestFail(app, "complete");
    const status = coordinator.snapshot() orelse return selfTestFail(app, "status");
    if (!status.valid() or status.state != @intFromEnum(contract.State.installed))
        return selfTestFail(app, "contract");
    app.system().println("UPDSVC selftest: OK");
    return 0;
}

fn selfTestFail(app: *r4os.App, reason: []const u8) i32 {
    app.system().write("UPDSVC selftest FAILED: ");
    app.system().println(reason);
    return 1;
}
