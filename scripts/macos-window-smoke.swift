import CoreGraphics
import Foundation

guard CommandLine.arguments.count == 2, let pid = Int32(CommandLine.arguments[1]) else {
    fputs("usage: macos-window-smoke.swift <pid>\n", stderr)
    exit(64)
}

let windows = CGWindowListCopyWindowInfo(
    [.optionOnScreenOnly, .excludeDesktopElements],
    kCGNullWindowID
) as? [[String: Any]] ?? []

for window in windows {
    guard let ownerPID = window[kCGWindowOwnerPID as String] as? Int32,
          ownerPID == pid,
          let layer = window[kCGWindowLayer as String] as? Int,
          layer == 0,
          let name = window[kCGWindowName as String] as? String,
          !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
        continue
    }
    print(name)
    exit(0)
}

fputs("no visible Rivet window for pid \(pid)\n", stderr)
exit(1)
