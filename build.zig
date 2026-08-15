const std = @import("std");

pub fn build(b: *std.Build) void {
    const sdk_build = b.lazyImport(@This(), "r4os_sdk") orelse return;
    const sdk_dep = b.dependencyFromBuildZig(sdk_build, .{});
    const libraries_build = b.lazyImport(@This(), "r4os_libraries") orelse return;
    const libraries_dep = b.dependencyFromBuildZig(libraries_build, .{});
    const sdk = sdk_build.sdk(b, sdk_dep, .{});
    _ = sdk.addR4MFWithOptions(b.path("module.R4MF"), .{
        .zig_module_roots = &.{
            sdk_dep.namedLazyPath("system_update_engine"),
            sdk_dep.namedLazyPath("update_service_contract"),
            libraries_dep.namedLazyPath("r4std_zig_binding"),
        },
    });

    const host_r4os = sdk.createR4osModule(b.graph.host, .Debug);
    const r4std_abi = b.createModule(.{
        .root_source_file = libraries_dep.path("R4STD/Bindings/Zig/r4std_abi.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    r4std_abi.addImport("r4os", host_r4os);
    const r4std = b.createModule(.{
        .root_source_file = libraries_dep.path("R4STD/Bindings/Zig/r4std.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    r4std.addImport("r4os", host_r4os);
    r4std.addImport("r4std_abi.zig", r4std_abi);
    const implementation = b.createModule(.{
        .root_source_file = libraries_dep.path("R4STD/Contract/Generated/implementation_abi.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    implementation.addImport("r4os", host_r4os);
    const provider = b.createModule(.{
        .root_source_file = libraries_dep.path("R4STD/Source/main.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    provider.addImport("r4os", host_r4os);
    provider.addImport("r4l_contract", implementation);
    const r4std_test = b.createModule(.{
        .root_source_file = libraries_dep.path("R4STD/Tests/consumer_runtime.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    r4std_test.addImport("provider", provider);
    r4std_test.addImport("r4os", host_r4os);
    r4std_test.addImport("r4std", r4std);
    const download = b.createModule(.{
        .root_source_file = b.path("src/update_download.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    download.addImport("r4std", r4std);
    download.addImport("r4std_test", r4std_test);
    const download_tests = b.addTest(.{ .root_module = download });
    const run_download_tests = b.addRunArtifact(download_tests);
    const test_step = b.step("test", "Run UpdateService host tests");
    test_step.dependOn(&run_download_tests.step);
}
