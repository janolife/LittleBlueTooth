//
//  LBTLog.swift
//  LittleBlueTooth
//
//  Structured logging types for LittleBlueTooth.
//

import Foundation

/// Log severity level
public enum LBTLogLevel: Int, Comparable, Sendable {
    case trace, debug, info, warning, error

    public static func < (lhs: LBTLogLevel, rhs: LBTLogLevel) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

/// Log category for filtering
public enum LBTLogCategory: String, Sendable {
    case connection
    case scan
    case gatt
    case l2cap
    case restore
}

/// Log handler signature for LittleBlueTooth
public typealias LBTLogHandler = @Sendable (_ message: String, _ level: LBTLogLevel, _ category: LBTLogCategory) -> Void
