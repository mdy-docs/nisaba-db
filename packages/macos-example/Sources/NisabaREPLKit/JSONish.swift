import Foundation
import NisabaDB

/// The relaxed, mongosh-flavored value grammar REPL arguments arrive in:
/// unquoted keys, single or double quotes, ObjectId("..."), ISODate("..."),
/// new Date(...). One parser instance walks one input string; the shell
/// command parser (ShellCommand.swift) drives it and checks what's left.
struct JSONishParser {
    enum ParseError: Error, CustomStringConvertible {
        case unexpected(String, at: Int)
        case badNumber(at: Int)
        case badObjectId(at: Int)
        case badDate(at: Int)
        case unterminatedString(at: Int)

        var description: String {
            switch self {
            case .unexpected(let what, let at): return "unexpected \(what) at position \(at)"
            case .badNumber(let at): return "malformed number at position \(at)"
            case .badObjectId(let at): return "ObjectId(...) needs 24 hex characters (position \(at))"
            case .badDate(let at): return "unrecognized date at position \(at)"
            case .unterminatedString(let at): return "unterminated string starting at position \(at)"
            }
        }
    }

    private let chars: [Character]
    private(set) var pos = 0

    init(_ text: String) {
        self.chars = Array(text)
    }

    // ---- primitives ------------------------------------------------------

    private func peek(_ ahead: Int = 0) -> Character? {
        pos + ahead < chars.count ? chars[pos + ahead] : nil
    }

    mutating func skipWhitespace() {
        while let c = peek(), c.isWhitespace { pos += 1 }
    }

    /// Consume `c` if it is next (after whitespace); says whether it was.
    mutating func take(_ c: Character) -> Bool {
        skipWhitespace()
        if peek() == c { pos += 1; return true }
        return false
    }

    mutating func expect(_ c: Character) throws {
        skipWhitespace()
        guard peek() == c else {
            throw ParseError.unexpected(peek().map { "'\($0)'" } ?? "end of input", at: pos)
        }
        pos += 1
    }

    var atEnd: Bool {
        var p = pos
        while p < chars.count, chars[p].isWhitespace { p += 1 }
        return p == chars.count
    }

    private static func isIdentStart(_ c: Character) -> Bool {
        c.isLetter || c == "_" || c == "$"
    }
    private static func isIdent(_ c: Character) -> Bool {
        c.isLetter || c.isNumber || c == "_" || c == "$"
    }

    mutating func identifier() -> String? {
        skipWhitespace()
        guard let c = peek(), Self.isIdentStart(c) else { return nil }
        var out = ""
        while let c = peek(), Self.isIdent(c) { out.append(c); pos += 1 }
        return out
    }

    // ---- values ----------------------------------------------------------

    /// Zero or more comma-separated values up to (not consuming) `)`.
    mutating func argumentList() throws -> [BJValue] {
        var args: [BJValue] = []
        skipWhitespace()
        if peek() == ")" { return args }
        while true {
            args.append(try value())
            if !take(",") { break }
        }
        return args
    }

    mutating func value() throws -> BJValue {
        skipWhitespace()
        guard let c = peek() else {
            throw ParseError.unexpected("end of input", at: pos)
        }
        switch c {
        case "{": return try object()
        case "[": return try array()
        case "\"", "'": return .string(try stringLiteral())
        case "-", "0"..."9": return try number()
        default:
            let start = pos
            guard let word = identifier() else {
                throw ParseError.unexpected("'\(c)'", at: pos)
            }
            switch word {
            case "true": return .bool(true)
            case "false": return .bool(false)
            case "null", "undefined": return .null
            case "ObjectId": return try objectIdCall(at: start)
            case "ISODate", "Date": return try dateCall(at: start)
            case "new":
                skipWhitespace()
                let inner = pos
                guard identifier() == "Date" else {
                    throw ParseError.unexpected("'new' (only `new Date(...)` is understood)", at: inner)
                }
                return try dateCall(at: inner)
            case "NumberLong", "NumberInt":
                try expect("(")
                skipWhitespace()
                let v: BJValue
                if let q = peek(), q == "\"" || q == "'" {
                    let s = try stringLiteral()
                    guard let n = Int64(s) else { throw ParseError.badNumber(at: pos) }
                    v = .int(n)
                } else {
                    guard case .int(let n) = try number() else {
                        throw ParseError.badNumber(at: pos)
                    }
                    v = .int(n)
                }
                try expect(")")
                return v
            default:
                throw ParseError.unexpected("'\(word)'", at: start)
            }
        }
    }

    private mutating func object() throws -> BJValue {
        try expect("{")
        var pairs: [(String, BJValue)] = []
        if take("}") { return .object(pairs) }
        while true {
            skipWhitespace()
            let key: String
            if let q = peek(), q == "\"" || q == "'" {
                key = try stringLiteral()
            } else if let ident = identifier() {
                key = ident
            } else {
                throw ParseError.unexpected(peek().map { "'\($0)'" } ?? "end of input", at: pos)
            }
            try expect(":")
            pairs.append((key, try value()))
            if take(",") {
                // tolerate a trailing comma, as the shell does
                if take("}") { return .object(pairs) }
                continue
            }
            try expect("}")
            return .object(pairs)
        }
    }

    private mutating func array() throws -> BJValue {
        try expect("[")
        var items: [BJValue] = []
        if take("]") { return .array(items) }
        while true {
            items.append(try value())
            if take(",") {
                if take("]") { return .array(items) }
                continue
            }
            try expect("]")
            return .array(items)
        }
    }

    mutating func stringLiteral() throws -> String {
        skipWhitespace()
        let start = pos
        guard let quote = peek(), quote == "\"" || quote == "'" else {
            throw ParseError.unexpected(peek().map { "'\($0)'" } ?? "end of input", at: pos)
        }
        pos += 1
        var out = ""
        while let c = peek() {
            pos += 1
            if c == quote { return out }
            if c == "\\" {
                guard let esc = peek() else { break }
                pos += 1
                switch esc {
                case "n": out.append("\n")
                case "t": out.append("\t")
                case "r": out.append("\r")
                case "\\", "\"", "'", "/": out.append(esc)
                case "u":
                    var hex = ""
                    for _ in 0..<4 {
                        guard let h = peek() else { throw ParseError.unterminatedString(at: start) }
                        hex.append(h); pos += 1
                    }
                    guard let code = UInt32(hex, radix: 16),
                          let scalar = Unicode.Scalar(code) else {
                        throw ParseError.unexpected("'\\u\(hex)'", at: pos)
                    }
                    out.append(Character(scalar))
                default:
                    out.append(esc)
                }
                continue
            }
            out.append(c)
        }
        throw ParseError.unterminatedString(at: start)
    }

    private mutating func number() throws -> BJValue {
        skipWhitespace()
        let start = pos
        var text = ""
        if peek() == "-" { text.append("-"); pos += 1 }
        var isFloat = false
        while let c = peek() {
            if c.isNumber {
                text.append(c); pos += 1
            } else if c == "." || c == "e" || c == "E" {
                isFloat = true
                text.append(c); pos += 1
                if c != ".", let sign = peek(), sign == "+" || sign == "-" {
                    text.append(sign); pos += 1
                }
            } else {
                break
            }
        }
        if !isFloat, let i = Int64(text) { return .int(i) }
        guard let d = Double(text) else { throw ParseError.badNumber(at: start) }
        return .double(d)
    }

    private mutating func objectIdCall(at start: Int) throws -> BJValue {
        try expect("(")
        skipWhitespace()
        if take(")") { return .objectId(ObjectId()) }
        let hex = try stringLiteral()
        guard let oid = ObjectId(hex: hex) else { throw ParseError.badObjectId(at: start) }
        try expect(")")
        return .objectId(oid)
    }

    /// ISODate("..."), Date(...), new Date(...): no argument is now, a
    /// number is epoch milliseconds, a string is ISO-8601.
    private mutating func dateCall(at start: Int) throws -> BJValue {
        try expect("(")
        skipWhitespace()
        if take(")") {
            return .date(Int64(Date().timeIntervalSince1970 * 1000))
        }
        if let q = peek(), q == "\"" || q == "'" {
            let text = try stringLiteral()
            guard let ms = Self.parseISO(text) else { throw ParseError.badDate(at: start) }
            try expect(")")
            return .date(ms)
        }
        guard case .int(let ms)? = try? number() else { throw ParseError.badDate(at: start) }
        try expect(")")
        return .date(ms)
    }

    static func parseISO(_ text: String) -> Int64? {
        let withFraction = ISO8601DateFormatter()
        withFraction.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let plain = ISO8601DateFormatter()
        plain.formatOptions = [.withInternetDateTime]
        // A bare date ("2026-08-04") is a midnight-UTC instant, as in JS.
        let dateOnly = ISO8601DateFormatter()
        dateOnly.formatOptions = [.withFullDate]
        for f in [withFraction, plain, dateOnly] {
            if let d = f.date(from: text) {
                return Int64((d.timeIntervalSince1970 * 1000).rounded())
            }
        }
        return nil
    }
}
