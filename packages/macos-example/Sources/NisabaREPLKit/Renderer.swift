import Foundation
import NisabaDB

/// BJValue -> the text a mongosh user expects: unquoted identifier keys,
/// ObjectId('...'), ISODate('...'), 2-space indents.
enum Renderer {
    static func render(_ value: BJValue) -> String {
        var out = ""
        write(value, indent: 0, into: &out)
        return out
    }

    /// An array of documents, one per paragraph — how find() results read.
    static func renderDocs(_ docs: [BJValue]) -> String {
        if docs.isEmpty { return "(no documents)" }
        return docs.map { render($0) }.joined(separator: "\n")
    }

    private static let isoFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()

    private static func write(_ value: BJValue, indent: Int, into out: inout String) {
        let pad = String(repeating: "  ", count: indent)
        let inner = String(repeating: "  ", count: indent + 1)
        switch value {
        case .null:
            out += "null"
        case .bool(let b):
            out += b ? "true" : "false"
        case .int(let i):
            out += String(i)
        case .double(let d):
            if d == d.rounded(), abs(d) < 1e15 {
                out += String(Int64(d))
            } else {
                out += String(d)
            }
        case .string(let s):
            out += quoted(s)
        case .binary(let data):
            out += "Binary('\(data.base64EncodedString())')"
        case .objectId(let oid):
            out += "ObjectId('\(oid.hex)')"
        case .date(let ms):
            let date = Date(timeIntervalSince1970: Double(ms) / 1000)
            out += "ISODate('\(isoFormatter.string(from: date))')"
        case .array(let items):
            if items.isEmpty { out += "[]"; return }
            out += "[\n"
            for (i, item) in items.enumerated() {
                out += inner
                write(item, indent: indent + 1, into: &out)
                out += i == items.count - 1 ? "\n" : ",\n"
            }
            out += pad + "]"
        case .object(let pairs):
            if pairs.isEmpty { out += "{}"; return }
            out += "{\n"
            for (i, (key, item)) in pairs.enumerated() {
                out += inner + renderKey(key) + ": "
                write(item, indent: indent + 1, into: &out)
                out += i == pairs.count - 1 ? "\n" : ",\n"
            }
            out += pad + "}"
        }
    }

    private static func renderKey(_ key: String) -> String {
        let identifierSafe = !key.isEmpty
            && !(key.first!.isNumber)
            && key.allSatisfy { $0.isLetter || $0.isNumber || $0 == "_" || $0 == "$" }
        return identifierSafe ? key : quoted(key)
    }

    private static func quoted(_ s: String) -> String {
        var out = "'"
        for c in s {
            switch c {
            case "'": out += "\\'"
            case "\\": out += "\\\\"
            case "\n": out += "\\n"
            case "\t": out += "\\t"
            case "\r": out += "\\r"
            default: out.append(c)
            }
        }
        return out + "'"
    }
}
