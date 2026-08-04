import CNisaba
import Foundation

public enum NisabaError: Error, CustomStringConvertible {
    /// The root directory could not be opened.
    case open(String)
    /// A DC_ERR_* / BJ_ERR_* code from opening or handling, with the
    /// engine's own sentence for it (dc_strerror).
    case engine(Int32, String)
    /// The engine answered { ok: false, code, msg } — the request was
    /// wrong or refused. The response is still a response; this carries
    /// what it said.
    case server(code: Int64, message: String)

    public var description: String {
        switch self {
        case .open(let path): return "cannot open database root at \(path)"
        case .engine(let code, let msg): return "\(msg) (\(code))"
        case .server(let code, let msg): return "\(msg) (\(code))"
        }
    }
}

/// One embedded nisaba-db instance over a root directory — many databases,
/// each a subdirectory (db_instance.h). Every request names its database
/// in a `db` field; there is no ambient "current database" in the engine.
///
/// Requests and responses are binjson objects in the wire shape
/// db_request.c owns, e.g.
///
///     try nisaba.call(["op": "find", "db": "app", "coll": "users",
///                      "filter": ["age": ["$gte": 21]]])
///
/// NOT thread-safe: the engine is single-threaded C. Confine an instance
/// to one thread/actor (the REPL app wraps it in one).
public final class Nisaba {
    public static let defaultOrder = Int32(DC_DEFAULT_ORDER)

    private var instance: OpaquePointer?
    private var rootState: OpaquePointer?
    private var rootFd: Int32 = -1

    /// Open (creating the directory if needed) an instance rooted at
    /// `rootPath`. `order` is the B+ tree order for files this process
    /// creates; existing files carry their own.
    public init(rootPath: String, order: Int32 = Nisaba.defaultOrder) throws {
        try FileManager.default.createDirectory(
            atPath: rootPath, withIntermediateDirectories: true)
        let fd = open(rootPath, O_RDONLY)
        guard fd >= 0 else { throw NisabaError.open(rootPath) }
        guard let st = root_new(fd) else {
            close(fd)
            throw NisabaError.open(rootPath)
        }
        var ops = dbi_root()
        root_fill(st, &ops)
        var inst: OpaquePointer?
        let rc = dbi_open(&ops, order, &inst)
        guard rc == BJ_OK, inst != nil else {
            root_free(st)
            close(fd)
            throw NisabaError.engine(rc, Nisaba.strerror(rc))
        }
        self.rootFd = fd
        self.rootState = st
        self.instance = inst
    }

    deinit { shutdown() }

    /// Close everything. Safe to call more than once; the instance is
    /// unusable afterward.
    public func shutdown() {
        if let inst = instance { dbi_close(inst) }
        instance = nil
        if let st = rootState { root_free(st) }
        rootState = nil
        if rootFd >= 0 { close(rootFd) }
        rootFd = -1
    }

    /// The engine's sentence for a DC_ERR_* / BJ_ERR_* code.
    public static func strerror(_ code: Int32) -> String {
        String(cString: dc_strerror(code))
    }

    /// Perform one request, returning the raw response object — including
    /// refusals, which arrive as { ok: false, code, msg }. `client` scopes
    /// cursor/stream ownership; any stable token will do.
    public func handle(_ request: BJValue, client: UInt64 = 1) throws -> BJValue {
        guard let inst = instance else {
            throw NisabaError.engine(BJ_ERR_STATE, "instance is closed")
        }
        let bytes = try BinJSON.encode(request)
        var out = dbuf()
        defer { dbuf_free(&out) }
        let rc = bytes.withUnsafeBytes { raw in
            dbi_handle(inst, client,
                       raw.bindMemory(to: UInt8.self).baseAddress,
                       bytes.count, &out)
        }
        // A nonzero return means no response could be built at all (OOM);
        // every refusal is a response (db_session.h's dbs_handle comment).
        guard rc == BJ_OK else {
            throw NisabaError.engine(rc, Nisaba.strerror(rc))
        }
        return try BinJSON.decode(Data(bytes: out.data, count: out.len))
    }

    /// Like handle, but unwraps { ok: false } into a thrown error.
    @discardableResult
    public func call(_ request: BJValue, client: UInt64 = 1) throws -> BJValue {
        let res = try handle(request, client: client)
        if res["ok"]?.boolValue == false {
            throw NisabaError.server(
                code: res["code"]?.intValue ?? 0,
                message: res["msg"]?.stringValue ?? "request refused")
        }
        return res
    }
}
