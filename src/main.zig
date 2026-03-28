const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;
const max_command_args = 64;

const ServerRole = enum {
    master,
    slave,
};

const RespCommand = struct {
    name: []const u8,
    args: [max_command_args][]const u8,
    arg_count: usize,
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

            role = .slave;
        }
    }

    const address = try net.Address.resolveIp("127.0.0.1", port);
    var database = Database.init(allocator);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.writeAll("accepted new connection\n");

        const thread = try std.Thread.spawn(.{}, handleConnection, .{ connection, &database, role });
        thread.detach();
    }
}

fn handleConnection(connection: std.net.Server.Connection, database: *Database, role: ServerRole) !void {
    defer connection.stream.close();

    var buffer: [1024]u8 = undefined;
    var in_transaction = false;
    var queued_commands: std.ArrayList(QueuedCommand) = .empty;
    defer {
        clearQueuedCommands(database.allocator, &queued_commands);
        queued_commands.deinit(database.allocator);
    }
    while (true) {
        const bytes_read = connection.stream.read(&buffer) catch 0;
        if (bytes_read == 0) {
            break;
        }

        const command = parseCommand(buffer[0..bytes_read]) orelse continue;

        if (std.ascii.eqlIgnoreCase(command.name, "multi")) {
            in_transaction = true;
            try connection.stream.writeAll("+OK\r\n");
        } else if (std.ascii.eqlIgnoreCase(command.name, "exec")) {
            if (!in_transaction) {
                try connection.stream.writeAll("-ERR EXEC without MULTI\r\n");
                continue;
            }

            in_transaction = false;
            var header_buffer: [32]u8 = undefined;
            const header = try std.fmt.bufPrint(&header_buffer, "*{d}\r\n", .{queued_commands.items.len});
            try connection.stream.writeAll(header);

            for (queued_commands.items) |*queued_command| {
                try executeCommand(connection.stream, database, role, queued_command.toRespCommand());
            }

            clearQueuedCommands(database.allocator, &queued_commands);
        } else if (std.ascii.eqlIgnoreCase(command.name, "discard")) {
            if (!in_transaction) {
                try connection.stream.writeAll("-ERR DISCARD without MULTI\r\n");
                continue;
            }

            in_transaction = false;
            clearQueuedCommands(database.allocator, &queued_commands);
            try connection.stream.writeAll("+OK\r\n");
        } else if (in_transaction) {
            try queued_commands.append(database.allocator, try QueuedCommand.init(database.allocator, command));
            try connection.stream.writeAll("+QUEUED\r\n");
        } else {
            try executeCommand(connection.stream, database, role, command);
        }
    }
}

fn clearQueuedCommands(allocator: std.mem.Allocator, queued_commands: *std.ArrayList(QueuedCommand)) void {
    for (queued_commands.items) |*queued_command| {
        queued_command.deinit(allocator);
    }
    queued_commands.clearRetainingCapacity();
}

fn executeCommand(stream: anytype, database: *Database, role: ServerRole, command: RespCommand) !void {
    if (std.ascii.eqlIgnoreCase(command.name, "ping")) {
        try stream.writeAll("+PONG\r\n");
    } else if (std.ascii.eqlIgnoreCase(command.name, "info")) {
        if (command.arg_count == 0 or std.ascii.eqlIgnoreCase(command.args[0], "replication")) {
            try writeBulkString(stream, switch (role) {
                .master => "role:master",
                .slave => "role:slave",
            });
        }
    } else if (std.ascii.eqlIgnoreCase(command.name, "echo")) {
        if (command.arg_count < 1) return;
        const message = command.args[0];
        try writeBulkString(stream, message);
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
        try stream.writeAll("+OK\r\n");
    } else if (std.ascii.eqlIgnoreCase(command.name, "get")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];
        const value = database.get(key) orelse {
            try stream.writeAll("$-1\r\n");
            return;
        };

        try writeBulkString(stream, value);
    } else if (std.ascii.eqlIgnoreCase(command.name, "incr")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];
        const new_number = database.incr(key) catch |err| switch (err) {
            error.InvalidInteger => {
                try stream.writeAll("-ERR value is not an integer or out of range\r\n");
                return;
            },
            else => return err,
        };

        var integer_buffer: [32]u8 = undefined;
        const integer = try std.fmt.bufPrint(&integer_buffer, ":{d}\r\n", .{new_number});
        try stream.writeAll(integer);
    } else if (std.ascii.eqlIgnoreCase(command.name, "type")) {
        if (command.arg_count < 1) return;
        const key = command.args[0];

        if (database.isStream(key)) {
            try stream.writeAll("+stream\r\n");
        } else if (database.get(key) != null) {
            try stream.writeAll("+string\r\n");
        } else {
            try stream.writeAll("+none\r\n");
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
