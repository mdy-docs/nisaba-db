import SwiftUI
import AppKit

@main
struct NisabaREPLApp: App {
    @StateObject private var model = ReplModel(rootPath: Self.rootPath())

    init() {
        // Launched as a bare SwiftPM executable there is no app bundle,
        // so opt in to being a regular, focusable app with a window.
        NSApplication.shared.setActivationPolicy(.regular)
        DispatchQueue.main.async {
            NSApplication.shared.activate(ignoringOtherApps: true)
        }
    }

    var body: some Scene {
        WindowGroup("Nisaba REPL") {
            ContentView(model: model)
                .frame(minWidth: 640, minHeight: 420)
        }
    }

    /// The database root: --root <path> on the command line, else
    /// ~/Library/Application Support/NisabaREPL/data.
    static func rootPath() -> String {
        let args = CommandLine.arguments
        if let i = args.firstIndex(of: "--root"), i + 1 < args.count {
            return (args[i + 1] as NSString).expandingTildeInPath
        }
        let support = FileManager.default.urls(
            for: .applicationSupportDirectory, in: .userDomainMask).first!
        return support.appendingPathComponent("NisabaREPL/data").path
    }
}
