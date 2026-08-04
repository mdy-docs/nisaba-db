import Foundation

/// A binjson value — the document model nisaba-db stores and answers in.
/// Mirrors the wire types in third_party/binjson/FORMAT.md; POINTER is an
/// internal storage type that never appears in requests or responses, so
/// it has no case here (decoding one is an error).
public indirect enum BJValue: Equatable {
    case null
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case string(String)
    case binary(Data)
    case objectId(ObjectId)
    /// Milliseconds since the Unix epoch — the DATE wire type.
    case date(Int64)
    case array([BJValue])
    /// Key order is preserved: binjson objects are ordered, and operator
    /// documents ({$set: ...}) care.
    case object([(String, BJValue)])

    public static func == (lhs: BJValue, rhs: BJValue) -> Bool {
        switch (lhs, rhs) {
        case (.null, .null): return true
        case let (.bool(a), .bool(b)): return a == b
        case let (.int(a), .int(b)): return a == b
        case let (.double(a), .double(b)): return a == b
        case let (.string(a), .string(b)): return a == b
        case let (.binary(a), .binary(b)): return a == b
        case let (.objectId(a), .objectId(b)): return a == b
        case let (.date(a), .date(b)): return a == b
        case let (.array(a), .array(b)): return a == b
        case let (.object(a), .object(b)):
            return a.count == b.count && zip(a, b).allSatisfy { $0.0 == $1.0 && $0.1 == $1.1 }
        default: return false
        }
    }

    /// Object field lookup (first match; binjson allows duplicate keys but
    /// nothing in this codebase produces them).
    public subscript(key: String) -> BJValue? {
        if case .object(let pairs) = self {
            return pairs.first(where: { $0.0 == key })?.1
        }
        return nil
    }

    public var arrayValue: [BJValue]? {
        if case .array(let a) = self { return a }
        return nil
    }

    public var stringValue: String? {
        if case .string(let s) = self { return s }
        return nil
    }

    public var intValue: Int64? {
        switch self {
        case .int(let i): return i
        case .double(let d) where d == d.rounded(): return Int64(exactly: d)
        default: return nil
        }
    }

    public var boolValue: Bool? {
        if case .bool(let b) = self { return b }
        return nil
    }

    public var isTruthy: Bool {
        switch self {
        case .null: return false
        case .bool(let b): return b
        case .int(let i): return i != 0
        case .double(let d): return d != 0
        default: return true
        }
    }
}

extension BJValue: ExpressibleByNilLiteral, ExpressibleByBooleanLiteral,
                   ExpressibleByIntegerLiteral, ExpressibleByFloatLiteral,
                   ExpressibleByStringLiteral, ExpressibleByArrayLiteral,
                   ExpressibleByDictionaryLiteral {
    public init(nilLiteral: ()) { self = .null }
    public init(booleanLiteral value: Bool) { self = .bool(value) }
    public init(integerLiteral value: Int64) { self = .int(value) }
    public init(floatLiteral value: Double) { self = .double(value) }
    public init(stringLiteral value: String) { self = .string(value) }
    public init(arrayLiteral elements: BJValue...) { self = .array(elements) }
    /// Dictionary literals keep their written order — Swift hands the
    /// elements over in source order, and we never pass through Dictionary.
    public init(dictionaryLiteral elements: (String, BJValue)...) {
        self = .object(elements.map { $0 })
    }
}
