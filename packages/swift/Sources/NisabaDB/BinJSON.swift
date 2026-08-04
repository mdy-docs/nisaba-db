import CNisaba
import Foundation

public enum BinJSONError: Error, CustomStringConvertible {
    case encode(Int32)
    case decode(Int32)
    /// POINTER is a storage-internal type; a request/response never
    /// carries one, so meeting it in a decode means the bytes are not a
    /// document.
    case unsupportedType

    public var description: String {
        switch self {
        case .encode(let c): return "binjson encode failed (\(c))"
        case .decode(let c): return "binjson decode failed (\(c))"
        case .unsupportedType: return "binjson value of a storage-internal type"
        }
    }
}

/// Encode/decode between BJValue trees and binjson bytes, through the C
/// codec itself (bj_builder / bj_decode) — one implementation of the
/// format per process, exactly like the JS host.
public enum BinJSON {
    public static func encode(_ value: BJValue) throws -> Data {
        guard let b = bj_builder_new() else { throw BinJSONError.encode(BJ_ERR_OOM) }
        defer { bj_builder_free(b) }
        try put(value, into: b)
        var len = 0
        guard let data = bj_builder_data(b, &len) else {
            throw BinJSONError.encode(bj_builder_error(b))
        }
        return Data(bytes: data, count: len)
    }

    private static func check(_ rc: Int32) throws {
        if rc != BJ_OK { throw BinJSONError.encode(rc) }
    }

    private static func put(_ value: BJValue, into b: OpaquePointer) throws {
        switch value {
        case .null:
            try check(bj_put_null(b))
        case .bool(let v):
            try check(bj_put_bool(b, v ? 1 : 0))
        case .int(let v):
            // INT is range-checked to JS safe integers; anything wider
            // travels as FLOAT, the same narrowing a JS host applies.
            if v > BJ_MAX_SAFE_INT || v < BJ_MIN_SAFE_INT {
                try check(bj_put_float(b, Double(v)))
            } else {
                try check(bj_put_int(b, v))
            }
        case .double(let v):
            try check(bj_put_float(b, v))
        case .string(let s):
            let utf8 = Array(s.utf8)
            try check(utf8.withUnsafeBufferPointer {
                bj_put_string(b, $0.baseAddress, UInt32($0.count))
            })
        case .binary(let d):
            try check(d.withUnsafeBytes {
                bj_put_binary(b, $0.bindMemory(to: UInt8.self).baseAddress, UInt32(d.count))
            })
        case .objectId(let oid):
            try check(oid.bytes.withUnsafeBufferPointer { bj_put_oid(b, $0.baseAddress) })
        case .date(let ms):
            try check(bj_put_date(b, ms))
        case .array(let items):
            try check(bj_begin_array(b))
            for item in items { try put(item, into: b) }
            try check(bj_end_array(b))
        case .object(let pairs):
            try check(bj_begin_object(b))
            for (key, item) in pairs {
                let utf8 = Array(key.utf8)
                try check(utf8.withUnsafeBufferPointer {
                    bj_put_key(b, $0.baseAddress, UInt32($0.count))
                })
                try put(item, into: b)
            }
            try check(bj_end_object(b))
        }
    }

    // ---- decoding --------------------------------------------------------

    private final class Decoder {
        enum Frame {
            case array([BJValue])
            case object([(String, BJValue)], key: String?)
        }
        var stack: [Frame] = []
        var result: BJValue?
        var sawUnsupported = false

        func emit(_ v: BJValue) {
            switch stack.popLast() {
            case nil:
                result = v
            case .array(var items):
                items.append(v)
                stack.append(.array(items))
            case .object(var pairs, let key):
                pairs.append((key ?? "", v))
                stack.append(.object(pairs, key: nil))
            }
        }
    }

    public static func decode(_ data: Data) throws -> BJValue {
        let state = Decoder()
        var visitor = bj_visitor()
        visitor.on_null = { ctx in Self.state(ctx).emit(.null) }
        visitor.on_bool = { ctx, t in Self.state(ctx).emit(.bool(t != 0)) }
        visitor.on_int = { ctx, v in Self.state(ctx).emit(.int(Int64(v))) }
        visitor.on_float = { ctx, v in Self.state(ctx).emit(.double(v)) }
        visitor.on_string = { ctx, p, len in
            let s = p.map { String(decoding: UnsafeBufferPointer(start: $0, count: Int(len)), as: UTF8.self) } ?? ""
            Self.state(ctx).emit(.string(s))
        }
        visitor.on_binary = { ctx, p, len in
            let d = p.map { Data(bytes: $0, count: Int(len)) } ?? Data()
            Self.state(ctx).emit(.binary(d))
        }
        visitor.on_oid = { ctx, p in
            let bytes = p.map { Array(UnsafeBufferPointer(start: $0, count: 12)) } ?? []
            if let oid = ObjectId(bytes: bytes) {
                Self.state(ctx).emit(.objectId(oid))
            } else {
                Self.state(ctx).sawUnsupported = true
            }
        }
        visitor.on_date = { ctx, ms in Self.state(ctx).emit(.date(Int64(ms))) }
        visitor.on_pointer = { ctx, _ in Self.state(ctx).sawUnsupported = true }
        visitor.on_array_begin = { ctx, _ in Self.state(ctx).stack.append(.array([])) }
        visitor.on_array_end = { ctx in
            let st = Self.state(ctx)
            if case .array(let items)? = st.stack.popLast() {
                st.emit(.array(items))
            }
        }
        visitor.on_object_begin = { ctx, _ in Self.state(ctx).stack.append(.object([], key: nil)) }
        visitor.on_key = { ctx, p, len in
            let st = Self.state(ctx)
            let s = p.map { String(decoding: UnsafeBufferPointer(start: $0, count: Int(len)), as: UTF8.self) } ?? ""
            if case .object(let pairs, _)? = st.stack.popLast() {
                st.stack.append(.object(pairs, key: s))
            }
        }
        visitor.on_object_end = { ctx in
            let st = Self.state(ctx)
            if case .object(let pairs, _)? = st.stack.popLast() {
                st.emit(.object(pairs))
            }
        }

        let rc = withExtendedLifetime(state) { () -> Int32 in
            visitor.ctx = Unmanaged.passUnretained(state).toOpaque()
            return data.withUnsafeBytes { raw in
                bj_decode(raw.bindMemory(to: UInt8.self).baseAddress, data.count, &visitor, nil)
            }
        }
        if rc != BJ_OK { throw BinJSONError.decode(rc) }
        if state.sawUnsupported { throw BinJSONError.unsupportedType }
        guard let result = state.result else { throw BinJSONError.decode(BJ_ERR_EOF) }
        return result
    }

    private static func state(_ ctx: UnsafeMutableRawPointer?) -> Decoder {
        Unmanaged<Decoder>.fromOpaque(ctx!).takeUnretainedValue()
    }
}
