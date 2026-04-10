//
//  Log.swift
//  LittleBlueTooth
//

import Foundation
import Combine

extension LittleBlueTooth {
    /// Emit a structured log message through the logHandler.
    func emit(_ message: String, _ level: LBTLogLevel, _ category: LBTLogCategory) {
        logHandler?(message, level, category)
    }
}

extension Publisher {
    /// Log a message through LittleBlueTooth's structured logHandler when this publisher is subscribed to.
    func log(_ lbt: LittleBlueTooth, _ message: @autoclosure @escaping () -> String, _ level: LBTLogLevel, _ category: LBTLogCategory) -> Publishers.HandleEvents<Self> {
        handleEvents(receiveSubscription: { _ in lbt.emit(message(), level, category) })
    }
}
