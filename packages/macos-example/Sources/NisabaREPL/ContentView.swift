import SwiftUI

struct ContentView: View {
    @ObservedObject var model: ReplModel
    @FocusState private var inputFocused: Bool

    var body: some View {
        VStack(spacing: 0) {
            transcript
            Divider()
            inputBar
        }
        .background(Color(nsColor: .textBackgroundColor))
        .toolbar {
            ToolbarItem(placement: .navigation) {
                Label(model.currentDb, systemImage: "cylinder.split.1x2")
                    .labelStyle(.titleAndIcon)
            }
            ToolbarItem(placement: .automatic) {
                Button("Clear") { model.clear() }
                    .keyboardShortcut("k", modifiers: .command)
            }
        }
        .navigationSubtitle(model.rootPath)
        .onAppear { inputFocused = true }
    }

    private var transcript: some View {
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 10) {
                    ForEach(model.entries) { entry in
                        VStack(alignment: .leading, spacing: 2) {
                            if !entry.command.isEmpty {
                                Text("\(model.currentDb)> \(entry.command)")
                                    .foregroundStyle(.secondary)
                            }
                            if !entry.output.isEmpty {
                                Text(entry.output)
                                    .foregroundStyle(entry.isError ? Color.red : Color.primary)
                                    .textSelection(.enabled)
                            }
                        }
                        .id(entry.id)
                    }
                }
                .font(.system(.body, design: .monospaced))
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(12)
            }
            .onChange(of: model.entries.count) {
                if let last = model.entries.last?.id {
                    withAnimation { proxy.scrollTo(last, anchor: .bottom) }
                }
            }
        }
    }

    private var inputBar: some View {
        HStack(spacing: 8) {
            Text("\(model.currentDb)>")
                .foregroundStyle(.secondary)
            TextField("db.collection.find({...})", text: $model.input)
                .textFieldStyle(.plain)
                .autocorrectionDisabled()
                .focused($inputFocused)
                .onSubmit {
                    model.submit()
                    inputFocused = true
                }
                .onKeyPress(.upArrow) {
                    model.historyUp()
                    return .handled
                }
                .onKeyPress(.downArrow) {
                    model.historyDown()
                    return .handled
                }
            if model.busy {
                ProgressView().controlSize(.small)
            }
        }
        .font(.system(.body, design: .monospaced))
        .padding(10)
    }
}
