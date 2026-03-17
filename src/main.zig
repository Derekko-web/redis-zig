const std = @import("std");
const stdout = std.fs.File.stdout();
const net = std.net;

const RespCommand = struct {
    name: []const u8,
    first_arg: ?[]const u8,
    second_arg: ?[]const u8,
};

const Entry = struct {
    key: []u8,
    value: []u8,
};

const Database = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(Entry),
    mutex: std.Thread.Mutex = .{},

    fn init(allocator: std.mem.Allocator) Database {
        return .{
            .allocator = allocator,
            .entries = .empty,
        };
    }

    fn set(self: *Database, key: []const u8, value: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                self.allocator.free(entry.value);
                entry.value = try self.allocator.dupe(u8, value);
                return;
            }
        }

        try self.entries.append(self.allocator, .{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, value),
        });
    }

    fn get(self: *Database, key: []const u8) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                return entry.value;
            }
        }

        return null;
    }
};

pub fn main() !void {
    const address = try net.Address.resolveIp("127.0.0.1", 6379);
    var database = Database.init(std.heap.page_allocator);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();

        try stdout.writeAll("accepted new connection\n");

        const thread = try std.Thread.spawn(.{}, handleConnection, .{ connection, &database });
        thread.detach();
    }
}

fn handleConnection(connection: std.net.Server.Connection, database: *Database) !void {
    defer connection.stream.close();

    var buffer: [1024]u8 = undefined;
    while (true) {
        const bytes_read = connection.stream.read(&buffer) catch 0;
        if (bytes_read == 0) {
            break;
        }

        const command = parseCommand(buffer[0..bytes_read]) orelse continue;

        if (std.ascii.eqlIgnoreCase(command.name, "ping")) {
            try connection.stream.writeAll("+PONG\r\n");
        } else if (std.ascii.eqlIgnoreCase(command.name, "echo")) {
            const message = command.first_arg orelse continue;
            try writeBulkString(connection.stream, message);
        } else if (std.ascii.eqlIgnoreCase(command.name, "set")) {
            const key = command.first_arg orelse continue;
            const value = command.second_arg orelse continue;

            try database.set(key, value);
            try connection.stream.writeAll("+OK\r\n");
        } else if (std.ascii.eqlIgnoreCase(command.name, "get")) {
            const key = command.first_arg orelse continue;
            const value = database.get(key) orelse {
                try connection.stream.writeAll("$-1\r\n");
                continue;
            };

            try writeBulkString(connection.stream, value);
        }
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

    const name = nextBulkString(&lines) orelse return null;
    const first_arg = if (element_count > 1) nextBulkString(&lines) else null;
    const second_arg = if (element_count > 2) nextBulkString(&lines) else null;

    return .{
        .name = name,
        .first_arg = first_arg,
        .second_arg = second_arg,
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

fn writeBulkString(stream: anytype, value: []const u8) !void {
    var header_buffer: [32]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buffer, "${d}\r\n", .{value.len});

    try stream.writeAll(header);
    try stream.writeAll(value);
    try stream.writeAll("\r\n");
}
