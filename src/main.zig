const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;
const max_command_args = 64;
const default_master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const default_master_repl_offset: u64 = 0;
const empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

const ServerRole = enum {
    master,
    slave,
};

const ReplicaOf = struct {
    host: []const u8,
    port: u16,
};

const ServerConfig = struct {
    dir: []const u8 = "",
    dbfilename: []const u8 = "",
};

const ReplicaState = struct {
    stream: *net.Stream,
    ack_offset: u64 = 0,
};

const ReplicaRegistry = struct {
    allocator: std.mem.Allocator,
    replicas: std.ArrayList(ReplicaState),
    replication_offset: u64 = 0,
    mutex: std.Thread.Mutex = .{},

    fn init(allocator: std.mem.Allocator) ReplicaRegistry {
        return .{
            .allocator = allocator,
            .replicas = .empty,
        };
    }

    fn register(self: *ReplicaRegistry, stream: *net.Stream) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.replicas.items) |replica| {
            if (replica.stream == stream) {
                return;
            }
        }

        try self.replicas.append(self.allocator, .{
            .stream = stream,
        });
    }

    fn unregister(self: *ReplicaRegistry, stream: *net.Stream) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.replicas.items, 0..) |replica, index| {
            if (replica.stream == stream) {
                _ = self.replicas.orderedRemove(index);
                return;
            }
        }
    }

    fn propagate(self: *ReplicaRegistry, command: RespCommand) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.replication_offset += respCommandLength(command);

        var index: usize = 0;
        while (index < self.replicas.items.len) {
            const stream = self.replicas.items[index].stream;
            writeRespCommand(stream, command) catch {
                _ = self.replicas.orderedRemove(index);
                continue;
            };

            index += 1;
        }
    }

    fn count(self: *ReplicaRegistry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.replicas.items.len;
    }

    fn updateAck(self: *ReplicaRegistry, stream: *net.Stream, ack_offset: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.replicas.items) |*replica| {
            if (replica.stream == stream) {
                replica.ack_offset = ack_offset;
                return;
            }
        }
    }

    fn replicationOffset(self: *ReplicaRegistry) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.replication_offset;
    }

    fn countAcked(self: *ReplicaRegistry, target_offset: u64) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.countAckedLocked(target_offset);
    }

    fn countAckedLocked(self: *ReplicaRegistry, target_offset: u64) usize {
        var acknowledged_replicas: usize = 0;
        for (self.replicas.items) |replica| {
            if (replica.ack_offset >= target_offset) {
                acknowledged_replicas += 1;
            }
        }

        return acknowledged_replicas;
    }

    fn requestAcks(self: *ReplicaRegistry) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var index: usize = 0;
        while (index < self.replicas.items.len) {
            const stream = self.replicas.items[index].stream;
            writeRespArrayCommand(stream, &.{ "REPLCONF", "GETACK", "*" }) catch {
                _ = self.replicas.orderedRemove(index);
                continue;
            };

            index += 1;
        }
    }

    fn waitForAcks(self: *ReplicaRegistry, target_offset: u64, requested_replicas: usize, timeout_ms: u64) !usize {
        try self.requestAcks();

        if (timeout_ms == 0) {
            while (true) {
                const acknowledged_replicas = self.countAcked(target_offset);
                if (acknowledged_replicas >= requested_replicas) {
                    return acknowledged_replicas;
                }

                std.Thread.sleep(std.time.ns_per_ms);
            }
        }

        const deadline = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
        while (std.time.milliTimestamp() < deadline) {
            const acknowledged_replicas = self.countAcked(target_offset);
            if (acknowledged_replicas >= requested_replicas) {
                return acknowledged_replicas;
            }

            std.Thread.sleep(std.time.ns_per_ms);
        }

        return self.countAcked(target_offset);
    }
};

const RespCommand = struct {
    name: []const u8,
    args: [max_command_args][]const u8,
    arg_count: usize,
};

const ParsedCommand = struct {
    command: RespCommand,
    bytes_consumed: usize,
};

const PubSubChannel = struct {
    name: []u8,
    subscribers: std.ArrayList(*net.Stream),
};

const PubSubRegistry = struct {
    allocator: std.mem.Allocator,
    channels: std.ArrayList(PubSubChannel),
    mutex: std.Thread.Mutex = .{},

    fn init(allocator: std.mem.Allocator) PubSubRegistry {
        return .{
            .allocator = allocator,
            .channels = .empty,
        };
    }

    fn subscribe(self: *PubSubRegistry, channel: []const u8, stream: *net.Stream) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.channels.items) |*existing_channel| {
            if (std.mem.eql(u8, existing_channel.name, channel)) {
                for (existing_channel.subscribers.items) |subscriber| {
                    if (subscriber == stream) {
                        return;
                    }
                }

                try existing_channel.subscribers.append(self.allocator, stream);
                return;
            }
        }

        var subscribers: std.ArrayList(*net.Stream) = .empty;
        errdefer subscribers.deinit(self.allocator);
        try subscribers.append(self.allocator, stream);

        try self.channels.append(self.allocator, .{
            .name = try self.allocator.dupe(u8, channel),
            .subscribers = subscribers,
        });
    }

    fn unsubscribe(self: *PubSubRegistry, channel: []const u8, stream: *net.Stream) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.channels.items, 0..) |*existing_channel, index| {
            if (!std.mem.eql(u8, existing_channel.name, channel)) {
                continue;
            }

            for (existing_channel.subscribers.items, 0..) |subscriber, subscriber_index| {
                if (subscriber == stream) {
                    _ = existing_channel.subscribers.orderedRemove(subscriber_index);
                    break;
                }
            }

            if (existing_channel.subscribers.items.len == 0) {
                existing_channel.subscribers.deinit(self.allocator);
                self.allocator.free(existing_channel.name);
                _ = self.channels.orderedRemove(index);
            }
            return;
        }
    }

    fn countSubscribers(self: *PubSubRegistry, channel: []const u8) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.channels.items) |existing_channel| {
            if (std.mem.eql(u8, existing_channel.name, channel)) {
                return existing_channel.subscribers.items.len;
            }
        }

        return 0;
    }

    fn publish(self: *PubSubRegistry, channel: []const u8, message: []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.channels.items, 0..) |*existing_channel, channel_index| {
            if (!std.mem.eql(u8, existing_channel.name, channel)) {
                continue;
            }

            var delivered_count: usize = 0;
            var subscriber_index: usize = 0;
            while (subscriber_index < existing_channel.subscribers.items.len) {
                const subscriber = existing_channel.subscribers.items[subscriber_index];
                writePubSubMessage(subscriber, channel, message) catch {
                    _ = existing_channel.subscribers.orderedRemove(subscriber_index);
                    continue;
                };

                delivered_count += 1;
                subscriber_index += 1;
            }

            if (existing_channel.subscribers.items.len == 0) {
                existing_channel.subscribers.deinit(self.allocator);
                self.allocator.free(existing_channel.name);
                _ = self.channels.orderedRemove(channel_index);
            }

            return delivered_count;
        }

        return 0;
    }
};

const ClientSubscriptions = struct {
    allocator: std.mem.Allocator,
    channels: std.ArrayList([]u8),

    fn init(allocator: std.mem.Allocator) ClientSubscriptions {
        return .{
            .allocator = allocator,
            .channels = .empty,
        };
    }

    fn deinit(self: *ClientSubscriptions, pubsub: *PubSubRegistry, stream: *net.Stream) void {
        self.clear(pubsub, stream);
        self.channels.deinit(self.allocator);
    }

    fn subscribe(self: *ClientSubscriptions, pubsub: *PubSubRegistry, stream: *net.Stream, channel: []const u8) !usize {
        for (self.channels.items) |existing_channel| {
            if (std.mem.eql(u8, existing_channel, channel)) {
                return self.channels.items.len;
            }
        }

        const owned_channel = try self.allocator.dupe(u8, channel);
        errdefer self.allocator.free(owned_channel);

        try self.channels.append(self.allocator, owned_channel);
        errdefer _ = self.channels.pop();

        try pubsub.subscribe(channel, stream);
        return self.channels.items.len;
    }

    fn count(self: *const ClientSubscriptions) usize {
        return self.channels.items.len;
    }

    fn clear(self: *ClientSubscriptions, pubsub: *PubSubRegistry, stream: *net.Stream) void {
        for (self.channels.items) |channel| {
            pubsub.unsubscribe(channel, stream);
            self.allocator.free(channel);
        }
        self.channels.clearRetainingCapacity();
    }
};

const QueuedCommand = struct {
    name: []u8,
    args: std.ArrayList([]u8),

    fn init(allocator: std.mem.Allocator, command: RespCommand) !QueuedCommand {
        var args: std.ArrayList([]u8) = .empty;
        errdefer {
            for (args.items) |arg| {
                allocator.free(arg);
            }
            args.deinit(allocator);
        }

        var arg_index: usize = 0;
        while (arg_index < command.arg_count) : (arg_index += 1) {
            try args.append(allocator, try allocator.dupe(u8, command.args[arg_index]));
        }

        return .{
            .name = try allocator.dupe(u8, command.name),
            .args = args,
        };
    }

    fn deinit(self: *QueuedCommand, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.args.items) |arg| {
            allocator.free(arg);
        }
        self.args.deinit(allocator);
    }

    fn toRespCommand(self: *const QueuedCommand) RespCommand {
        var args: [max_command_args][]const u8 = undefined;
        for (self.args.items, 0..) |arg, index| {
            args[index] = arg;
        }

        return .{
            .name = self.name,
            .args = args,
            .arg_count = self.args.items.len,
        };
    }
};

const Entry = struct {
    key: []u8,
    value: []u8,
    expires_at_ms: ?i64,
};

const ListEntry = struct {
    key: []u8,
    value: []u8,
};

const StreamField = struct {
    field: []u8,
    value: []u8,
};

const StreamEntry = struct {
    key: []u8,
    id: []u8,
    fields: std.ArrayList(StreamField),
};

const StreamId = struct {
    milliseconds_time: u64,
    sequence_number: u64,
};

const BlpopWaiter = struct {
    key: []u8,
    mutex: std.Thread.Mutex = .{},
    condition: std.Thread.Condition = .{},
    ready: bool = false,
    value: ?[]u8 = null,
};

const Database = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(Entry),
    lists: std.ArrayList(ListEntry),
    streams: std.ArrayList(StreamEntry),
    waiters: std.ArrayList(*BlpopWaiter),
    mutex: std.Thread.Mutex = .{},

    fn init(allocator: std.mem.Allocator) Database {
        return .{
            .allocator = allocator,
            .entries = .empty,
            .lists = .empty,
            .streams = .empty,
            .waiters = .empty,
        };
    }

    fn set(self: *Database, key: []const u8, value: []const u8, expires_at_ms: ?i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                self.allocator.free(entry.value);
                entry.value = try self.allocator.dupe(u8, value);
                entry.expires_at_ms = expires_at_ms;
                return;
            }
        }

        try self.entries.append(self.allocator, .{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, value),
            .expires_at_ms = expires_at_ms,
        });
    }

    fn get(self: *Database, key: []const u8) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                if (entry.expires_at_ms) |expires_at_ms| {
                    if (std.time.milliTimestamp() >= expires_at_ms) {
                        return null;
                    }
                }

                return entry.value;
            }
        }

        return null;
    }

    fn writeKeys(self: *Database, stream: anytype, pattern: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!std.mem.eql(u8, pattern, "*")) {
            try stream.writeAll("*0\r\n");
            return;
        }

        var key_count: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.expires_at_ms) |expires_at_ms| {
                if (std.time.milliTimestamp() >= expires_at_ms) {
                    continue;
                }
            }

            key_count += 1;
        }

        var header_buffer: [32]u8 = undefined;
        const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{key_count});
        try stream.writeAll(header);

        for (self.entries.items) |entry| {
            if (entry.expires_at_ms) |expires_at_ms| {
                if (std.time.milliTimestamp() >= expires_at_ms) {
                    continue;
                }
            }

            try writeBulkString(stream, entry.key);
        }
    }

    fn incr(self: *Database, key: []const u8) !i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            if (!std.mem.eql(u8, entry.key, key)) {
                continue;
            }

            const current_number = std.fmt.parseInt(i64, entry.value, 10) catch return error.InvalidInteger;
            const new_number = current_number + 1;

            var value_buffer: [32]u8 = undefined;
            const new_value = try std.fmt.bufPrint(&value_buffer, "{d}", .{new_number});

            self.allocator.free(entry.value);
            entry.value = try self.allocator.dupe(u8, new_value);
            return new_number;
        }

        try self.entries.append(self.allocator, .{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, "1"),
            .expires_at_ms = null,
        });

        return 1;
    }

    fn xadd(self: *Database, key: []const u8, id: []const u8, field_values: []const []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var stream_entry = StreamEntry{
            .key = try self.allocator.dupe(u8, key),
            .id = try self.allocator.dupe(u8, id),
            .fields = .empty,
        };

        var index: usize = 0;
        while (index + 1 < field_values.len) : (index += 2) {
            try stream_entry.fields.append(self.allocator, .{
                .field = try self.allocator.dupe(u8, field_values[index]),
                .value = try self.allocator.dupe(u8, field_values[index + 1]),
            });
        }

        try self.streams.append(self.allocator, stream_entry);
    }

    fn isStream(self: *Database, key: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.streams.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                return true;
            }
        }

        return false;
    }

    fn getLastStreamId(self: *Database, key: []const u8) ?StreamId {
        self.mutex.lock();
        defer self.mutex.unlock();

        var index = self.streams.items.len;
        while (index > 0) {
            index -= 1;
            const entry = self.streams.items[index];
            if (std.mem.eql(u8, entry.key, key)) {
                return parseStreamId(entry.id);
            }
        }

        return null;
    }

    fn getLastStreamSequenceNumber(self: *Database, key: []const u8, milliseconds_time: u64) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var index = self.streams.items.len;
        while (index > 0) {
            index -= 1;
            const entry = self.streams.items[index];
            if (!std.mem.eql(u8, entry.key, key)) {
                continue;
            }

            const stream_id = parseStreamId(entry.id) orelse continue;
            if (stream_id.milliseconds_time == milliseconds_time) {
                return stream_id.sequence_number;
            }
        }

        return null;
    }

    fn rpush(self: *Database, key: []const u8, values: []const []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        for (self.lists.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;
            }
        }

        var start_index: usize = 0;
        if (values.len > 0 and try self.wakeBlpopWaiterLocked(key, values[0])) {
            start_index = 1;
        }

        for (values[start_index..]) |value| {
            try self.lists.append(self.allocator, .{
                .key = try self.allocator.dupe(u8, key),
                .value = try self.allocator.dupe(u8, value),
            });
        }

        return list_len + values.len;
    }

    fn lpush(self: *Database, key: []const u8, values: []const []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        var insert_index = self.lists.items.len;
        var found_first = false;

        for (self.lists.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;

                if (!found_first) {
                    insert_index = index;
                    found_first = true;
                }
            }
        }

        for (values) |value| {
            try self.lists.insert(self.allocator, insert_index, .{
                .key = try self.allocator.dupe(u8, key),
                .value = try self.allocator.dupe(u8, value),
            });
        }

        return list_len + values.len;
    }

    fn llen(self: *Database, key: []const u8) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        for (self.lists.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;
            }
        }

        return list_len;
    }

    fn wakeBlpopWaiterLocked(self: *Database, key: []const u8, value: []const u8) !bool {
        for (self.waiters.items, 0..) |waiter, index| {
            if (std.mem.eql(u8, waiter.key, key)) {
                _ = self.waiters.orderedRemove(index);

                waiter.mutex.lock();
                defer waiter.mutex.unlock();

                waiter.value = try self.allocator.dupe(u8, value);
                waiter.ready = true;
                waiter.condition.signal();
                return true;
            }
        }

        return false;
    }

    fn removeBlpopWaiter(self: *Database, waiter: *BlpopWaiter) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.waiters.items, 0..) |queued_waiter, index| {
            if (queued_waiter == waiter) {
                _ = self.waiters.orderedRemove(index);
                return true;
            }
        }

        return false;
    }

    fn beginBlpop(self: *Database, key: []const u8, waiter: *BlpopWaiter) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.lists.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.key, key)) {
                const removed = self.lists.orderedRemove(index);
                self.allocator.free(removed.key);
                return removed.value;
            }
        }

        try self.waiters.append(self.allocator, waiter);
        return null;
    }

    fn lpop(self: *Database, key: []const u8) ?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.lists.items, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.key, key)) {
                const removed = self.lists.orderedRemove(index);
                self.allocator.free(removed.key);
                return removed.value;
            }
        }

        return null;
    }

    fn writeLRange(self: *Database, stream: anytype, key: []const u8, start: i64, stop: i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list_len: usize = 0;
        for (self.lists.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                list_len += 1;
            }
        }

        if (list_len == 0) {
            try stream.writeAll("*0\r\n");
            return;
        }

        const list_len_i64: i64 = @intCast(list_len);
        var resolved_start = start;
        if (resolved_start < 0) {
            resolved_start += list_len_i64;
        }
        if (resolved_start < 0) {
            resolved_start = 0;
        }

        var resolved_stop = stop;
        if (resolved_stop < 0) {
            resolved_stop += list_len_i64;
        }
        if (resolved_stop < 0) {
            resolved_stop = 0;
        }

        if (resolved_start >= list_len_i64 or resolved_start > resolved_stop) {
            try stream.writeAll("*0\r\n");
            return;
        }

        if (resolved_stop >= list_len_i64) {
            resolved_stop = list_len_i64 - 1;
        }

        const range_start: usize = @intCast(resolved_start);
        const range_end: usize = @intCast(resolved_stop);
        const result_len = range_end - range_start + 1;

        var header_buffer: [32]u8 = undefined;
        const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{result_len});
        try stream.writeAll(header);

        var list_index: usize = 0;
        for (self.lists.items) |entry| {
            if (!std.mem.eql(u8, entry.key, key)) {
                continue;
            }

            if (list_index >= range_start and list_index <= range_end) {
                try writeBulkString(stream, entry.value);
            }

            list_index += 1;
            if (list_index > range_end) {
                break;
            }
        }
    }

    fn writeXRange(self: *Database, stream: anytype, key: []const u8, start_id: StreamId, end_id: StreamId) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result_len: usize = 0;
        for (self.streams.items) |entry| {
            if (!std.mem.eql(u8, entry.key, key)) {
                continue;
            }

            const entry_id = parseStreamId(entry.id) orelse continue;
            if (compareStreamIds(entry_id, start_id) < 0) {
                continue;
            }

            if (compareStreamIds(entry_id, end_id) > 0) {
                continue;
            }

            result_len += 1;
        }

        var header_buffer: [32]u8 = undefined;
        const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{result_len});
        try stream.writeAll(header);

        for (self.streams.items) |entry| {
            if (!std.mem.eql(u8, entry.key, key)) {
                continue;
            }

            const entry_id = parseStreamId(entry.id) orelse continue;
            if (compareStreamIds(entry_id, start_id) < 0) {
                continue;
            }

            if (compareStreamIds(entry_id, end_id) > 0) {
                continue;
            }

            try stream.writeAll("*2\r\n");
            try writeBulkString(stream, entry.id);

            const field_count = entry.fields.items.len * 2;
            const field_header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{field_count});
            try stream.writeAll(field_header);

            for (entry.fields.items) |field| {
                try writeBulkString(stream, field.field);
                try writeBulkString(stream, field.value);
            }
        }
    }

    fn countXReadEntriesLocked(self: *Database, key: []const u8, start_id: StreamId) usize {
        var result_len: usize = 0;
        for (self.streams.items) |entry| {
            if (!std.mem.eql(u8, entry.key, key)) {
                continue;
            }

            const entry_id = parseStreamId(entry.id) orelse continue;
            if (compareStreamIds(entry_id, start_id) <= 0) {
                continue;
            }

            result_len += 1;
        }

        return result_len;
    }

    fn hasXReadEntries(self: *Database, keys: []const []const u8, start_ids: []const StreamId) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        var stream_index: usize = 0;
        while (stream_index < keys.len) : (stream_index += 1) {
            if (self.countXReadEntriesLocked(keys[stream_index], start_ids[stream_index]) > 0) {
                return true;
            }
        }

        return false;
    }

    fn writeXRead(self: *Database, stream: anytype, keys: []const []const u8, start_ids: []const StreamId) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var stream_count: usize = 0;
        var stream_index: usize = 0;
        while (stream_index < keys.len) : (stream_index += 1) {
            if (self.countXReadEntriesLocked(keys[stream_index], start_ids[stream_index]) > 0) {
                stream_count += 1;
            }
        }

        if (stream_count == 0) {
            try stream.writeAll("*-1\r\n");
            return;
        }

        var header_buffer: [32]u8 = undefined;
        const streams_header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{stream_count});
        try stream.writeAll(streams_header);

        stream_index = 0;
        while (stream_index < keys.len) : (stream_index += 1) {
            const key = keys[stream_index];
            const start_id = start_ids[stream_index];
            const result_len = self.countXReadEntriesLocked(key, start_id);
            if (result_len == 0) {
                continue;
            }

            try stream.writeAll("*2\r\n");
            try writeBulkString(stream, key);

            const entries_header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{result_len});
            try stream.writeAll(entries_header);

            for (self.streams.items) |entry| {
                if (!std.mem.eql(u8, entry.key, key)) {
                    continue;
                }

                const entry_id = parseStreamId(entry.id) orelse continue;
                if (compareStreamIds(entry_id, start_id) <= 0) {
                    continue;
                }

                try stream.writeAll("*2\r\n");
                try writeBulkString(stream, entry.id);

                const field_count = entry.fields.items.len * 2;
                const field_header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{field_count});
                try stream.writeAll(field_header);

                for (entry.fields.items) |field| {
                    try writeBulkString(stream, field.field);
                    try writeBulkString(stream, field.value);
                }
            }
        }
    }
};

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var port: u16 = 6379;
    var role: ServerRole = .master;
    var replicaof: ?ReplicaOf = null;
    var config = ServerConfig{};
    var arg_index: usize = 1;
    while (arg_index < args.len) : (arg_index += 1) {
        if (std.mem.eql(u8, args[arg_index], "--port")) {
            arg_index += 1;
            if (arg_index >= args.len) {
                return error.MissingPortValue;
            }

            port = try std.fmt.parseInt(u16, args[arg_index], 10);
        } else if (std.mem.eql(u8, args[arg_index], "--replicaof")) {
            arg_index += 1;
            if (arg_index >= args.len) {
                return error.MissingReplicaOfValue;
            }

            var parts = std.mem.tokenizeScalar(u8, args[arg_index], ' ');
            const host = parts.next() orelse return error.InvalidReplicaOfValue;
            const port_text = parts.next() orelse blk: {
                arg_index += 1;
                if (arg_index >= args.len) {
                    return error.MissingReplicaOfPort;
                }

                break :blk args[arg_index];
            };

            role = .slave;
            replicaof = .{
                .host = host,
                .port = try std.fmt.parseInt(u16, port_text, 10),
            };
        } else if (std.mem.eql(u8, args[arg_index], "--dir")) {
            arg_index += 1;
            if (arg_index >= args.len) {
                return error.MissingDirValue;
            }

            config.dir = args[arg_index];
        } else if (std.mem.eql(u8, args[arg_index], "--dbfilename")) {
            arg_index += 1;
            if (arg_index >= args.len) {
                return error.MissingDbFilenameValue;
            }

            config.dbfilename = args[arg_index];
        }
    }

    const address = try net.Address.resolveIp("127.0.0.1", port);
    var database = Database.init(allocator);
    try loadRdbFromConfig(allocator, &database, config);
    var replicas = ReplicaRegistry.init(allocator);
    var pubsub = PubSubRegistry.init(allocator);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    if (replicaof) |master| {
        const replication_thread = try std.Thread.spawn(.{}, performReplicationHandshake, .{ allocator, master, port, &database, &replicas });
        replication_thread.detach();
    }

    while (true) {
        const connection = try listener.accept();

        try stdout.writeAll("accepted new connection\n");

        const thread = try std.Thread.spawn(.{}, handleConnection, .{ connection, &database, &replicas, &pubsub, &config, role });
        thread.detach();
    }
}

fn handleConnection(connection: std.net.Server.Connection, database: *Database, replicas: *ReplicaRegistry, pubsub: *PubSubRegistry, config: *const ServerConfig, role: ServerRole) !void {
    var client = connection;
    defer {
        replicas.unregister(&client.stream);
        client.stream.close();
    }

    var buffer: [1024]u8 = undefined;
    var in_transaction = false;
    var queued_commands: std.ArrayList(QueuedCommand) = .empty;
    var subscriptions = ClientSubscriptions.init(database.allocator);
    defer {
        clearQueuedCommands(database.allocator, &queued_commands);
        queued_commands.deinit(database.allocator);
        subscriptions.deinit(pubsub, &client.stream);
    }
    while (true) {
        const bytes_read = client.stream.read(&buffer) catch 0;
        if (bytes_read == 0) {
            break;
        }

        const command = parseCommand(buffer[0..bytes_read]) orelse continue;
        if (subscriptions.count() > 0 and !isSubscribedModeCommandAllowed(command.name)) {
            var error_buffer: [256]u8 = undefined;
            const error_message = try std.fmt.bufPrint(&error_buffer, "-ERR Can't execute '{s}' in subscribed mode\r\n", .{command.name});
            try client.stream.writeAll(error_message);
            continue;
        }

        if (std.ascii.eqlIgnoreCase(command.name, "multi")) {
            in_transaction = true;
            try client.stream.writeAll("+OK\r\n");
        } else if (std.ascii.eqlIgnoreCase(command.name, "quit")) {
            try client.stream.writeAll("+OK\r\n");
            break;
        } else if (std.ascii.eqlIgnoreCase(command.name, "exec")) {
            if (!in_transaction) {
                try client.stream.writeAll("-ERR EXEC without MULTI\r\n");
                continue;
            }

            in_transaction = false;
            var header_buffer: [32]u8 = undefined;
            const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{queued_commands.items.len});
            try client.stream.writeAll(header);

            for (queued_commands.items) |*queued_command| {
                try executeCommand(&client.stream, database, replicas, pubsub, &subscriptions, config, role, queued_command.toRespCommand(), true, 0);
            }

            clearQueuedCommands(database.allocator, &queued_commands);
        } else if (std.ascii.eqlIgnoreCase(command.name, "discard")) {
            if (!in_transaction) {
                try client.stream.writeAll("-ERR DISCARD without MULTI\r\n");
                continue;
            }

            in_transaction = false;
            clearQueuedCommands(database.allocator, &queued_commands);
            try client.stream.writeAll("+OK\r\n");
        } else if (in_transaction) {
            try queued_commands.append(database.allocator, try QueuedCommand.init(database.allocator, command));
            try client.stream.writeAll("+QUEUED\r\n");
        } else {
            try executeCommand(&client.stream, database, replicas, pubsub, &subscriptions, config, role, command, true, 0);
        }
    }
}

fn clearQueuedCommands(allocator: std.mem.Allocator, queued_commands: *std.ArrayList(QueuedCommand)) void {
    for (queued_commands.items) |*queued_command| {
        queued_command.deinit(allocator);
    }
    queued_commands.clearRetainingCapacity();
}

fn isSubscribedModeCommandAllowed(command_name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(command_name, "subscribe") or
        std.ascii.eqlIgnoreCase(command_name, "unsubscribe") or
        std.ascii.eqlIgnoreCase(command_name, "psubscribe") or
        std.ascii.eqlIgnoreCase(command_name, "punsubscribe") or
        std.ascii.eqlIgnoreCase(command_name, "ping") or
        std.ascii.eqlIgnoreCase(command_name, "quit") or
        std.ascii.eqlIgnoreCase(command_name, "reset");
}

fn performReplicationHandshake(allocator: std.mem.Allocator, master: ReplicaOf, listening_port: u16, database: *Database, replicas: *ReplicaRegistry) !void {
    while (true) {
        var stream = net.tcpConnectToHost(allocator, master.host, master.port) catch {
            std.Thread.sleep(50 * std.time.ns_per_ms);
            continue;
        };
        defer stream.close();

        try writeRespArrayCommand(stream, &.{"PING"});
        try expectSimpleString(stream, "PONG");

        var port_buffer: [5]u8 = undefined;
        const port_text = try std.fmt.bufPrint(&port_buffer, "{d}", .{listening_port});
        try writeRespArrayCommand(stream, &.{ "REPLCONF", "listening-port", port_text });
        try expectSimpleString(stream, "OK");

        try writeRespArrayCommand(stream, &.{ "REPLCONF", "capa", "psync2" });
        try expectSimpleString(stream, "OK");

        try writeRespArrayCommand(stream, &.{ "PSYNC", "?", "-1" });
        try expectFullResync(stream);
        try skipRdbFile(stream);
        try processReplicationStream(&stream, database, replicas);
        return;
    }
}

fn writeRespArrayCommand(stream: anytype, args: []const []const u8) !void {
    var header_buffer: [32]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{args.len});
    try stream.writeAll(header);

    for (args) |arg| {
        try writeBulkString(stream, arg);
    }
}

fn writeRespCommand(stream: anytype, command: RespCommand) !void {
    var header_buffer: [32]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{command.arg_count + 1});
    try stream.writeAll(header);
    try writeBulkString(stream, command.name);

    var arg_index: usize = 0;
    while (arg_index < command.arg_count) : (arg_index += 1) {
        try writeBulkString(stream, command.args[arg_index]);
    }
}

fn writePubSubMessage(stream: anytype, channel: []const u8, message: []const u8) !void {
    try stream.writeAll("*3\r\n");
    try writeBulkString(stream, "message");
    try writeBulkString(stream, channel);
    try writeBulkString(stream, message);
}

fn respCommandLength(command: RespCommand) u64 {
    var total_len: u64 = std.fmt.count("*{d}\r\n", .{command.arg_count + 1});
    total_len += respBulkStringLength(command.name);

    var arg_index: usize = 0;
    while (arg_index < command.arg_count) : (arg_index += 1) {
        total_len += respBulkStringLength(command.args[arg_index]);
    }

    return total_len;
}

fn respBulkStringLength(value: []const u8) u64 {
    return std.fmt.count("${d}\r\n", .{value.len}) + value.len + 2;
}

fn readLine(stream: anytype, buffer: []u8) ![]const u8 {
    var line_len: usize = 0;

    while (true) {
        if (line_len == buffer.len) {
            return error.ReplicationResponseTooLong;
        }

        const bytes_read = try stream.read(buffer[line_len .. line_len + 1]);
        if (bytes_read == 0) {
            return error.UnexpectedEof;
        }

        line_len += bytes_read;
        if (line_len >= 2 and
            buffer[line_len - 2] == '\r' and
            buffer[line_len - 1] == '\n')
        {
            return buffer[0 .. line_len - 2];
        }
    }
}

fn expectSimpleString(stream: anytype, expected: []const u8) !void {
    var line_buffer: [64]u8 = undefined;
    const line = try readLine(stream, &line_buffer);
    var expected_buffer: [64]u8 = undefined;
    const expected_line = try std.fmt.bufPrint(&expected_buffer, "+{s}", .{expected});
    if (!std.mem.eql(u8, line, expected_line)) {
        return error.UnexpectedReplicationResponse;
    }
}

fn expectFullResync(stream: anytype) !void {
    var line_buffer: [128]u8 = undefined;
    const line = try readLine(stream, &line_buffer);
    if (!std.mem.startsWith(u8, line, "+FULLRESYNC ")) {
        return error.UnexpectedReplicationResponse;
    }
}

fn skipRdbFile(stream: anytype) !void {
    var header_buffer: [64]u8 = undefined;
    const header = try readLine(stream, &header_buffer);
    if (header.len < 2 or header[0] != '$') {
        return error.InvalidRdbTransfer;
    }

    var remaining = std.fmt.parseInt(usize, header[1..], 10) catch return error.InvalidRdbTransfer;
    var buffer: [1024]u8 = undefined;
    while (remaining > 0) {
        const chunk_len = @min(remaining, buffer.len);
        const bytes_read = try stream.read(buffer[0..chunk_len]);
        if (bytes_read == 0) {
            return error.UnexpectedEof;
        }

        remaining -= bytes_read;
    }
}

fn processReplicationStream(stream: *net.Stream, database: *Database, replicas: *ReplicaRegistry) !void {
    var read_buffer: [1024]u8 = undefined;
    var pending: std.ArrayList(u8) = .empty;
    var replication_offset: u64 = 0;
    const config = ServerConfig{};
    var pubsub = PubSubRegistry.init(database.allocator);
    var subscriptions = ClientSubscriptions.init(database.allocator);
    defer pending.deinit(database.allocator);
    defer subscriptions.deinit(&pubsub, stream);

    while (true) {
        const bytes_read = stream.read(&read_buffer) catch 0;
        if (bytes_read == 0) {
            break;
        }

        try pending.appendSlice(database.allocator, read_buffer[0..bytes_read]);

        while (true) {
            const parsed = parseNextCommand(pending.items) catch |err| switch (err) {
                error.Incomplete => break,
                error.Invalid => return error.InvalidReplicationCommand,
            };

            try executeCommand(stream, database, replicas, &pubsub, &subscriptions, &config, .slave, parsed.command, false, replication_offset);
            replication_offset += parsed.bytes_consumed;

            const remaining_len = pending.items.len - parsed.bytes_consumed;
            std.mem.copyForwards(u8, pending.items[0..remaining_len], pending.items[parsed.bytes_consumed..]);
            pending.shrinkRetainingCapacity(remaining_len);
        }
    }
}

fn executeCommand(stream: anytype, database: *Database, replicas: *ReplicaRegistry, pubsub: *PubSubRegistry, subscriptions: *ClientSubscriptions, config: *const ServerConfig, role: ServerRole, command: RespCommand, should_reply: bool, replication_offset: u64) !void {
    if (std.ascii.eqlIgnoreCase(command.name, "ping")) {
        if (should_reply) {
            if (subscriptions.count() > 0) {
                try stream.writeAll("*2\r\n");
                try writeBulkString(stream, "pong");
                try writeBulkString(stream, "");
            } else {
                try stream.writeAll("+PONG\r\n");
            }
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "replconf")) {
        if (!should_reply and role == .slave and command.arg_count >= 2 and
            std.ascii.eqlIgnoreCase(command.args[0], "getack") and
            std.mem.eql(u8, command.args[1], "*"))
        {
            var offset_buffer: [32]u8 = undefined;
            const offset_text = try std.fmt.bufPrint(&offset_buffer, "{d}", .{replication_offset});
            try writeRespArrayCommand(stream, &.{ "REPLCONF", "ACK", offset_text });
        } else if (role == .master and command.arg_count >= 2 and std.ascii.eqlIgnoreCase(command.args[0], "ack")) {
            const ack_offset = std.fmt.parseInt(u64, command.args[1], 10) catch return;
            replicas.updateAck(stream, ack_offset);
        } else if (should_reply) {
            try stream.writeAll("+OK\r\n");
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "psync")) {
        var fullresync_buffer: [96]u8 = undefined;
        const fullresync = try std.fmt.bufPrint(&fullresync_buffer, "+FULLRESYNC {s} {d}\r\n", .{
            default_master_replid,
            default_master_repl_offset,
        });
        try stream.writeAll(fullresync);
        try writeEmptyRdb(stream);
        try replicas.register(stream);
    } else if (std.ascii.eqlIgnoreCase(command.name, "info")) {
        if (command.arg_count == 0 or std.ascii.eqlIgnoreCase(command.args[0], "replication")) {
            if (should_reply) {
                try writeInfoReplication(stream, role);
            }
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "config")) {
        if (command.arg_count < 2) return;
        if (!std.ascii.eqlIgnoreCase(command.args[0], "get")) return;
        if (!should_reply) return;

        const parameter = command.args[1];
        if (std.ascii.eqlIgnoreCase(parameter, "dir")) {
            try stream.writeAll("*2\r\n");
            try writeBulkString(stream, "dir");
            try writeBulkString(stream, config.dir);
        } else if (std.ascii.eqlIgnoreCase(parameter, "dbfilename")) {
            try stream.writeAll("*2\r\n");
            try writeBulkString(stream, "dbfilename");
            try writeBulkString(stream, config.dbfilename);
        } else {
            try stream.writeAll("*0\r\n");
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "echo")) {
        if (command.arg_count < 1) return;
        const message = command.args[0];
        if (should_reply) {
            try writeBulkString(stream, message);
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "subscribe")) {
        if (command.arg_count < 1 or !should_reply) return;
        const subscription_count = try subscriptions.subscribe(pubsub, stream, command.args[0]);

        try stream.writeAll("*3\r\n");
        try writeBulkString(stream, "subscribe");
        try writeBulkString(stream, command.args[0]);
        var integer_buffer: [32]u8 = undefined;
        const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{subscription_count});
        try stream.writeAll(integer);
    } else if (std.ascii.eqlIgnoreCase(command.name, "reset")) {
        subscriptions.clear(pubsub, stream);
        if (should_reply) {
            try stream.writeAll("+RESET\r\n");
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "publish")) {
        if (command.arg_count < 2 or !should_reply) return;

        var integer_buffer: [32]u8 = undefined;
        const published_count = try pubsub.publish(command.args[0], command.args[1]);
        const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{published_count});
        try stream.writeAll(integer);
    } else if (std.ascii.eqlIgnoreCase(command.name, "set")) {
        if (command.arg_count < 2) return;
        const key = command.args[0];
        const value = command.args[1];
        var expires_at_ms: ?i64 = null;

        if (command.arg_count > 2) {
            if (command.arg_count < 4) return;
            const option = command.args[2];
            const option_value = command.args[3];

            if (!std.ascii.eqlIgnoreCase(option, "px")) {
                return;
            }

            const ttl_ms = std.fmt.parseInt(i64, option_value, 10) catch return;
            expires_at_ms = std.time.milliTimestamp() + ttl_ms;
        }

        try database.set(key, value, expires_at_ms);
        if (should_reply) {
            try stream.writeAll("+OK\r\n");
        }
        if (role == .master) {
            try replicas.propagate(command);
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "wait")) {
        if (command.arg_count < 2) return;
        const requested_replicas = std.fmt.parseInt(usize, command.args[0], 10) catch return;
        const timeout_ms = std.fmt.parseInt(u64, command.args[1], 10) catch return;

        if (should_reply) {
            const target_offset = replicas.replicationOffset();
            var acknowledged_replicas = replicas.countAcked(target_offset);

            if (replicas.count() == 0) {
                acknowledged_replicas = 0;
            } else if (target_offset > 0 and acknowledged_replicas < requested_replicas) {
                acknowledged_replicas = try replicas.waitForAcks(target_offset, requested_replicas, timeout_ms);
            }

            var integer_buffer: [32]u8 = undefined;
            const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{acknowledged_replicas});
            try stream.writeAll(integer);
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "get")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];
        const value = database.get(key) orelse {
            if (should_reply) {
                try stream.writeAll("$-1\r\n");
            }
            return;
        };

        if (should_reply) {
            try writeBulkString(stream, value);
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "keys")) {
        if (command.arg_count < 1) return;
        if (should_reply) {
            try database.writeKeys(stream, command.args[0]);
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "incr")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];
        const new_number = database.incr(key) catch |err| switch (err) {
            error.InvalidInteger => {
                if (should_reply) {
                    try stream.writeAll("-ERR value is not an integer or out of range\r\n");
                }
                return;
            },
            else => return err,
        };

        var integer_buffer: [32]u8 = undefined;
        const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{new_number});
        if (should_reply) {
            try stream.writeAll(integer);
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "type")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];

        if (database.isStream(key)) {
            if (should_reply) {
                try stream.writeAll("+stream\r\n");
            }
        } else if (database.get(key) != null) {
            if (should_reply) {
                try stream.writeAll("+string\r\n");
            }
        } else {
            if (should_reply) {
                try stream.writeAll("+none\r\n");
            }
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "xadd")) {
        if (command.arg_count < 4) return;
        if ((command.arg_count - 2) % 2 != 0) return;

        const key = command.args[0];
        const requested_id = command.args[1];
        var generated_id_buffer: [64]u8 = undefined;
        const id = if (std.mem.eql(u8, requested_id, "*")) blk: {
            const milliseconds_time: u64 = @intCast(std.time.milliTimestamp());
            var sequence_number: u64 = 0;
            if (database.getLastStreamSequenceNumber(key, milliseconds_time)) |last_sequence_number| {
                sequence_number = last_sequence_number + 1;
            }

            const generated_id = try std.fmt.bufPrint(&generated_id_buffer, "{d}-{d}", .{ milliseconds_time, sequence_number });
            break :blk generated_id;
        } else if (parseAutoSequenceMillisecondsTime(requested_id)) |milliseconds_time| blk: {
            var sequence_number: u64 = 0;
            if (database.getLastStreamSequenceNumber(key, milliseconds_time)) |last_sequence_number| {
                sequence_number = last_sequence_number + 1;
            } else if (milliseconds_time == 0) {
                sequence_number = 1;
            }

            const generated_id = try std.fmt.bufPrint(&generated_id_buffer, "{d}-{d}", .{ milliseconds_time, sequence_number });
            break :blk generated_id;
        } else requested_id;

        const stream_id = parseStreamId(id) orelse return;

        if (stream_id.milliseconds_time == 0 and stream_id.sequence_number == 0) {
            try stream.writeAll("-ERR The ID specified in XADD must be greater than 0-0\r\n");
            return;
        }

        if (database.getLastStreamId(key)) |last_stream_id| {
            if (compareStreamIds(stream_id, last_stream_id) <= 0) {
                try stream.writeAll("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                return;
            }
        }

        try database.xadd(key, id, command.args[2..command.arg_count]);
        try writeBulkString(stream, id);
    } else if (std.ascii.eqlIgnoreCase(command.name, "xrange")) {
        if (command.arg_count < 3) return;
        const key = command.args[0];
        const start_id = parseXRangeId(command.args[1], true) orelse return;
        const end_id = parseXRangeId(command.args[2], false) orelse return;

        try database.writeXRange(stream, key, start_id, end_id);
    } else if (std.ascii.eqlIgnoreCase(command.name, "xread")) {
        if (command.arg_count < 3) return;
        var block_timeout_ms: ?i64 = null;
        var streams_index: usize = 0;

        if (std.ascii.eqlIgnoreCase(command.args[0], "block")) {
            if (command.arg_count < 5) return;

            const parsed_timeout_ms = std.fmt.parseInt(i64, command.args[1], 10) catch return;
            if (parsed_timeout_ms < 0) return;

            block_timeout_ms = parsed_timeout_ms;
            streams_index = 2;
        }

        if (!std.ascii.eqlIgnoreCase(command.args[streams_index], "streams")) return;
        if ((command.arg_count - streams_index - 1) % 2 != 0) return;

        const stream_count = (command.arg_count - streams_index - 1) / 2;
        const keys = command.args[streams_index + 1 .. streams_index + 1 + stream_count];
        var start_ids: [max_command_args]StreamId = undefined;
        var stream_index: usize = 0;

        while (stream_index < stream_count) : (stream_index += 1) {
            const raw_start_id = command.args[streams_index + 1 + stream_count + stream_index];

            if (std.mem.eql(u8, raw_start_id, "$")) {
                start_ids[stream_index] = database.getLastStreamId(keys[stream_index]) orelse .{
                    .milliseconds_time = 0,
                    .sequence_number = 0,
                };
            } else {
                start_ids[stream_index] = parseStreamId(raw_start_id) orelse return;
            }
        }

        if (block_timeout_ms) |timeout_ms| {
            if (!database.hasXReadEntries(keys, start_ids[0..stream_count])) {
                if (timeout_ms == 0) {
                    while (!database.hasXReadEntries(keys, start_ids[0..stream_count])) {
                        std.Thread.sleep(std.time.ns_per_ms);
                    }
                } else {
                    const deadline = std.time.milliTimestamp() + timeout_ms;

                    while (std.time.milliTimestamp() < deadline) {
                        if (database.hasXReadEntries(keys, start_ids[0..stream_count])) {
                            break;
                        }

                        std.Thread.sleep(std.time.ns_per_ms);
                    }

                    if (!database.hasXReadEntries(keys, start_ids[0..stream_count])) {
                        try stream.writeAll("*-1\r\n");
                        return;
                    }
                }
            }
        }

        try database.writeXRead(stream, keys, start_ids[0..stream_count]);
    } else if (std.ascii.eqlIgnoreCase(command.name, "rpush")) {
        if (command.arg_count < 2) return;
        const key = command.args[0];

        const list_len = try database.rpush(key, command.args[1..command.arg_count]);

        var integer_buffer: [32]u8 = undefined;
        const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{list_len});
        try stream.writeAll(integer);
    } else if (std.ascii.eqlIgnoreCase(command.name, "lpush")) {
        if (command.arg_count < 2) return;
        const key = command.args[0];

        const list_len = try database.lpush(key, command.args[1..command.arg_count]);

        var integer_buffer: [32]u8 = undefined;
        const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{list_len});
        try stream.writeAll(integer);
    } else if (std.ascii.eqlIgnoreCase(command.name, "lrange")) {
        if (command.arg_count < 3) return;
        const key = command.args[0];
        const start = std.fmt.parseInt(i64, command.args[1], 10) catch return;
        const stop = std.fmt.parseInt(i64, command.args[2], 10) catch return;

        try database.writeLRange(stream, key, start, stop);
    } else if (std.ascii.eqlIgnoreCase(command.name, "llen")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];
        const list_len = database.llen(key);

        var integer_buffer: [32]u8 = undefined;
        const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{list_len});
        try stream.writeAll(integer);
    } else if (std.ascii.eqlIgnoreCase(command.name, "lpop")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];

        if (command.arg_count > 1) {
            const count = std.fmt.parseInt(usize, command.args[1], 10) catch return;
            var values: std.ArrayList([]u8) = .empty;
            defer {
                for (values.items) |value| {
                    database.allocator.free(value);
                }
                values.deinit(database.allocator);
            }

            var index: usize = 0;
            while (index < count) : (index += 1) {
                const value = database.lpop(key) orelse break;
                try values.append(database.allocator, value);
            }

            var header_buffer: [32]u8 = undefined;
            const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{values.items.len});
            try stream.writeAll(header);

            for (values.items) |value| {
                try writeBulkString(stream, value);
            }
        } else {
            const value = database.lpop(key) orelse {
                try stream.writeAll("$-1\r\n");
                return;
            };

            defer database.allocator.free(value);
            try writeBulkString(stream, value);
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "blpop")) {
        if (command.arg_count < 2) return;
        const key = command.args[0];
        const timeout_seconds = std.fmt.parseFloat(f64, command.args[1]) catch return;
        if (timeout_seconds < 0) return;

        if (timeout_seconds > 0) {
            const timeout_ms: i64 = @intFromFloat(timeout_seconds * 1000.0);
            const deadline = std.time.milliTimestamp() + timeout_ms;
            var got_value = false;

            while (std.time.milliTimestamp() < deadline) {
                if (database.lpop(key)) |value| {
                    defer database.allocator.free(value);
                    try writeBlpopResponse(stream, key, value);
                    got_value = true;
                    break;
                }

                std.Thread.sleep(std.time.ns_per_ms);
            }

            if (!got_value) {
                if (database.lpop(key)) |value| {
                    defer database.allocator.free(value);
                    try writeBlpopResponse(stream, key, value);
                } else {
                    try stream.writeAll("*-1\r\n");
                }
            }
            return;
        }

        var waiter = BlpopWaiter{
            .key = try database.allocator.dupe(u8, key),
        };
        defer database.allocator.free(waiter.key);

        waiter.mutex.lock();
        defer waiter.mutex.unlock();

        if (try database.beginBlpop(key, &waiter)) |value| {
            defer database.allocator.free(value);
            try writeBlpopResponse(stream, key, value);
            return;
        }

        while (!waiter.ready) {
            waiter.condition.wait(&waiter.mutex);
        }

        const value = waiter.value orelse return;
        defer database.allocator.free(value);
        try writeBlpopResponse(stream, waiter.key, value);
    }
}

fn writeInfoReplication(stream: anytype, role: ServerRole) !void {
    const role_name = switch (role) {
        .master => "master",
        .slave => "slave",
    };

    var info_buffer: [128]u8 = undefined;
    const info = try std.fmt.bufPrint(&info_buffer, "role:{s}\r\nmaster_replid:{s}\r\nmaster_repl_offset:{d}", .{
        role_name,
        default_master_replid,
        default_master_repl_offset,
    });

    try writeBulkString(stream, info);
}

fn writeEmptyRdb(stream: anytype) !void {
    var rdb_buffer: [empty_rdb_hex.len / 2]u8 = undefined;
    const rdb = try std.fmt.hexToBytes(&rdb_buffer, empty_rdb_hex);

    var header_buffer: [32]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buffer, "${d}\r\n", .{rdb.len});
    try stream.writeAll(header);
    try stream.writeAll(rdb);
}

fn loadRdbFromConfig(allocator: std.mem.Allocator, database: *Database, config: ServerConfig) !void {
    if (config.dir.len == 0 or config.dbfilename.len == 0) {
        return;
    }

    const rdb_path = try std.fs.path.join(allocator, &.{ config.dir, config.dbfilename });
    defer allocator.free(rdb_path);

    const file_contents = std.fs.cwd().readFileAlloc(allocator, rdb_path, 64 * 1024 * 1024) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer allocator.free(file_contents);

    try parseRdb(database, file_contents);
}

fn parseRdb(database: *Database, file_contents: []const u8) !void {
    if (file_contents.len < 9 or !std.mem.eql(u8, file_contents[0..5], "REDIS")) {
        return error.InvalidRdbFile;
    }

    var index: usize = 9;
    var expires_at_ms: ?i64 = null;

    while (index < file_contents.len) {
        const opcode = file_contents[index];
        index += 1;

        switch (opcode) {
            0xFA => {
                _ = try readRdbString(file_contents, &index);
                _ = try readRdbString(file_contents, &index);
            },
            0xFB => {
                _ = try readRdbLength(file_contents, &index);
                _ = try readRdbLength(file_contents, &index);
            },
            0xFC => {
                expires_at_ms = @bitCast(try readRdbInt(u64, file_contents, &index, .little));
            },
            0xFD => {
                const expires_at_seconds = try readRdbInt(u32, file_contents, &index, .little);
                expires_at_ms = @as(i64, expires_at_seconds) * 1000;
            },
            0xFE => {
                _ = try readRdbLength(file_contents, &index);
            },
            0xFF => break,
            0x00 => {
                const key = try readRdbString(file_contents, &index);
                const value = try readRdbString(file_contents, &index);
                try database.set(key, value, expires_at_ms);
                expires_at_ms = null;
            },
            else => return error.UnsupportedRdbValueType,
        }
    }
}

fn readRdbLength(file_contents: []const u8, index: *usize) !usize {
    if (index.* >= file_contents.len) {
        return error.InvalidRdbFile;
    }

    const first_byte = file_contents[index.*];
    index.* += 1;

    return switch (first_byte >> 6) {
        0b00 => first_byte & 0x3F,
        0b01 => blk: {
            if (index.* >= file_contents.len) {
                return error.InvalidRdbFile;
            }

            const value = (@as(usize, first_byte & 0x3F) << 8) | file_contents[index.*];
            index.* += 1;
            break :blk value;
        },
        0b10 => blk: {
            const value = try readRdbInt(u32, file_contents, index, .big);
            break :blk @as(usize, value);
        },
        0b11 => return error.UnsupportedRdbLengthEncoding,
        else => unreachable,
    };
}

fn readRdbString(file_contents: []const u8, index: *usize) ![]const u8 {
    if (index.* >= file_contents.len) {
        return error.InvalidRdbFile;
    }

    const first_byte = file_contents[index.*];
    const encoding_type = first_byte >> 6;
    if (encoding_type != 0b11) {
        const string_len = try readRdbLength(file_contents, index);
        const string_start = index.*;
        const string_end = string_start + string_len;
        if (string_end > file_contents.len) {
            return error.InvalidRdbFile;
        }

        index.* = string_end;
        return file_contents[string_start..string_end];
    }

    index.* += 1;
    return switch (first_byte & 0x3F) {
        0 => blk: {
            const value = try readRdbInt(i8, file_contents, index, .little);
            break :blk try formatRdbIntegerString(value);
        },
        1 => blk: {
            const value = try readRdbInt(i16, file_contents, index, .little);
            break :blk try formatRdbIntegerString(value);
        },
        2 => blk: {
            const value = try readRdbInt(i32, file_contents, index, .little);
            break :blk try formatRdbIntegerString(value);
        },
        else => return error.UnsupportedRdbStringEncoding,
    };
}

fn formatRdbIntegerString(value: anytype) ![]const u8 {
    return std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{value});
}

fn readRdbInt(comptime T: type, file_contents: []const u8, index: *usize, endian: std.builtin.Endian) !T {
    const byte_len = @sizeOf(T);
    const value_start = index.*;
    const value_end = value_start + byte_len;
    if (value_end > file_contents.len) {
        return error.InvalidRdbFile;
    }

    index.* = value_end;
    const bytes: *const [byte_len]u8 = file_contents[value_start..][0..byte_len];
    return std.mem.readInt(T, bytes, endian);
}

fn parseNextCommand(data: []const u8) error{ Incomplete, Invalid }!ParsedCommand {
    if (data.len == 0) {
        return error.Incomplete;
    }

    if (data[0] != '*') {
        return error.Invalid;
    }

    const array_header_end = findCrlf(data, 1) orelse return error.Incomplete;
    const element_count = std.fmt.parseInt(usize, data[1..array_header_end], 10) catch return error.Invalid;
    if (element_count == 0 or element_count - 1 > max_command_args) {
        return error.Invalid;
    }

    var index = array_header_end + 2;
    const name = try parseBulkStringAt(data, &index);
    var args: [max_command_args][]const u8 = undefined;
    var arg_count: usize = 0;

    while (arg_count < element_count - 1) : (arg_count += 1) {
        args[arg_count] = try parseBulkStringAt(data, &index);
    }

    return .{
        .command = .{
            .name = name,
            .args = args,
            .arg_count = arg_count,
        },
        .bytes_consumed = index,
    };
}

fn parseCommand(data: []const u8) ?RespCommand {
    var lines = std.mem.splitSequence(u8, data, "\r\n");

    const array_header = lines.next() orelse return null;
    if (array_header.len < 2 or array_header[0] != '*') {
        return null;
    }

    const element_count = std.fmt.parseInt(usize, array_header[1..], 10) catch return null;
    if (element_count == 0) {
        return null;
    }

    if (element_count - 1 > max_command_args) {
        return null;
    }

    const name = nextBulkString(&lines) orelse return null;
    var args: [max_command_args][]const u8 = undefined;
    var arg_count: usize = 0;

    while (arg_count < element_count - 1) : (arg_count += 1) {
        args[arg_count] = nextBulkString(&lines) orelse return null;
    }

    return .{
        .name = name,
        .args = args,
        .arg_count = arg_count,
    };
}

fn findCrlf(data: []const u8, start: usize) ?usize {
    var index = start;
    while (index + 1 < data.len) : (index += 1) {
        if (data[index] == '\r' and data[index + 1] == '\n') {
            return index;
        }
    }

    return null;
}

fn parseBulkStringAt(data: []const u8, index: *usize) error{ Incomplete, Invalid }![]const u8 {
    if (index.* >= data.len) {
        return error.Incomplete;
    }

    if (data[index.*] != '$') {
        return error.Invalid;
    }

    const header_start = index.* + 1;
    const header_end = findCrlf(data, header_start) orelse return error.Incomplete;
    const expected_len = std.fmt.parseInt(usize, data[header_start..header_end], 10) catch return error.Invalid;
    const value_start = header_end + 2;
    const value_end = value_start + expected_len;

    if (value_end + 2 > data.len) {
        return error.Incomplete;
    }

    if (data[value_end] != '\r' or data[value_end + 1] != '\n') {
        return error.Invalid;
    }

    index.* = value_end + 2;
    return data[value_start..value_end];
}

fn nextBulkString(lines: anytype) ?[]const u8 {
    const bulk_header = lines.next() orelse return null;
    if (bulk_header.len < 2 or bulk_header[0] != '$') {
        return null;
    }

    const expected_len = std.fmt.parseInt(usize, bulk_header[1..], 10) catch return null;
    const value = lines.next() orelse return null;
    if (value.len != expected_len) {
        return null;
    }

    return value;
}

fn parseStreamId(id: []const u8) ?StreamId {
    var parts = std.mem.splitScalar(u8, id, '-');
    const milliseconds_time_text = parts.next() orelse return null;
    const sequence_number_text = parts.next() orelse return null;

    if (parts.next() != null) {
        return null;
    }

    const milliseconds_time = std.fmt.parseInt(u64, milliseconds_time_text, 10) catch return null;
    const sequence_number = std.fmt.parseInt(u64, sequence_number_text, 10) catch return null;

    return .{
        .milliseconds_time = milliseconds_time,
        .sequence_number = sequence_number,
    };
}

fn parseAutoSequenceMillisecondsTime(id: []const u8) ?u64 {
    var parts = std.mem.splitScalar(u8, id, '-');
    const milliseconds_time_text = parts.next() orelse return null;
    const sequence_number_text = parts.next() orelse return null;

    if (parts.next() != null) {
        return null;
    }

    if (!std.mem.eql(u8, sequence_number_text, "*")) {
        return null;
    }

    return std.fmt.parseInt(u64, milliseconds_time_text, 10) catch return null;
}

fn parseXRangeId(id: []const u8, is_start: bool) ?StreamId {
    if (is_start and std.mem.eql(u8, id, "-")) {
        return .{
            .milliseconds_time = 0,
            .sequence_number = 0,
        };
    }

    if (!is_start and std.mem.eql(u8, id, "+")) {
        return .{
            .milliseconds_time = std.math.maxInt(u64),
            .sequence_number = std.math.maxInt(u64),
        };
    }

    var parts = std.mem.splitScalar(u8, id, '-');
    const milliseconds_time_text = parts.next() orelse return null;
    const maybe_sequence_number_text = parts.next();

    if (parts.next() != null) {
        return null;
    }

    const milliseconds_time = std.fmt.parseInt(u64, milliseconds_time_text, 10) catch return null;
    var sequence_number: u64 = 0;
    if (maybe_sequence_number_text) |sequence_number_text| {
        sequence_number = std.fmt.parseInt(u64, sequence_number_text, 10) catch return null;
    } else if (!is_start) {
        sequence_number = std.math.maxInt(u64);
    }

    return .{
        .milliseconds_time = milliseconds_time,
        .sequence_number = sequence_number,
    };
}

fn compareStreamIds(left: StreamId, right: StreamId) i2 {
    if (left.milliseconds_time < right.milliseconds_time) {
        return -1;
    }

    if (left.milliseconds_time > right.milliseconds_time) {
        return 1;
    }

    if (left.sequence_number < right.sequence_number) {
        return -1;
    }

    if (left.sequence_number > right.sequence_number) {
        return 1;
    }

    return 0;
}

fn writeBulkString(stream: anytype, value: []const u8) !void {
    var header_buffer: [32]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buffer, "${d}\r\n", .{value.len});

    try stream.writeAll(header);
    try stream.writeAll(value);
    try stream.writeAll("\r\n");
}

fn writeBlpopResponse(stream: anytype, key: []const u8, value: []const u8) !void {
    try stream.writeAll("*2\r\n");
    try writeBulkString(stream, key);
    try writeBulkString(stream, value);
}
