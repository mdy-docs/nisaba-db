import Foundation
import SwiftUI
import NisabaREPLKit

/// The transcript and input state, on the main actor; execution hops to
/// the ReplSession actor so the UI never blocks on disk.
@MainActor
final class ReplModel: ObservableObject {
    struct Entry: Identifiable {
        let id = UUID()
        let command: String
        var output: String
        var isError: Bool
    }

    @Published private(set) var entries: [Entry] = []
    @Published var input = ""
    @Published private(set) var currentDb = "test"
    @Published private(set) var busy = false

    let rootPath: String
    private let session: ReplSession
    private var history: [String] = []
    private var historyIndex: Int? = nil
    private var draft = ""

    init(rootPath: String) {
        self.rootPath = rootPath
        self.session = ReplSession(rootPath: rootPath)
        entries.append(Entry(
            command: "",
            output: "nisaba-db — data in \(rootPath)\nType `help` to see what this shell understands.",
            isError: false))
    }

    func submit() {
        let line = input.trimmingCharacters(in: .whitespacesAndNewlines)
        input = ""
        guard !line.isEmpty else { return }
        history.append(line)
        historyIndex = nil
        draft = ""

        if line == "clear" || line == "cls" {
            entries.removeAll()
            return
        }

        busy = true
        Task {
            let result = await session.execute(line)
            let db = await session.databaseName()
            self.currentDb = db
            self.entries.append(Entry(command: line,
                                      output: result.text,
                                      isError: result.isError))
            self.busy = false
        }
    }

    func historyUp() {
        guard !history.isEmpty else { return }
        if historyIndex == nil {
            draft = input
            historyIndex = history.count - 1
        } else if historyIndex! > 0 {
            historyIndex! -= 1
        }
        input = history[historyIndex!]
    }

    func historyDown() {
        guard let index = historyIndex else { return }
        if index < history.count - 1 {
            historyIndex = index + 1
            input = history[historyIndex!]
        } else {
            historyIndex = nil
            input = draft
        }
    }

    func clear() {
        entries.removeAll()
    }
}
