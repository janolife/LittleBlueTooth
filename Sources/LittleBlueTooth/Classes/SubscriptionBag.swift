//
//  SubscriptionBag.swift
//  LittleBlueTooth
//
//  Thread-safe replacement for [UUID: AnyCancellable]. All mutations
//  are guarded by an os_unfair_lock. Cancellation runs outside the
//  critical section so a sink that cancels one subscription cannot
//  deadlock by re-entering the bag.
//

import Foundation
import Combine

final class SubscriptionBag: @unchecked Sendable {

    private var storage: [UUID: AnyCancellable] = [:]
    private let lock: UnsafeMutablePointer<os_unfair_lock>

    init() {
        self.lock = UnsafeMutablePointer<os_unfair_lock>.allocate(capacity: 1)
        self.lock.initialize(to: os_unfair_lock())
    }

    deinit {
        let leftovers: [AnyCancellable] = withLock {
            let values = Array(storage.values)
            storage.removeAll()
            return values
        }
        for cancellable in leftovers { cancellable.cancel() }
        lock.deinitialize(count: 1)
        lock.deallocate()
    }

    func insert(_ cancellable: AnyCancellable, for key: UUID) {
        withLock { storage[key] = cancellable }
    }

    func remove(_ key: UUID) {
        let cancellable: AnyCancellable? = withLock {
            storage.removeValue(forKey: key)
        }
        cancellable?.cancel()
    }

    func cancelAll() {
        let all: [AnyCancellable] = withLock {
            let values = Array(storage.values)
            storage.removeAll()
            return values
        }
        for cancellable in all { cancellable.cancel() }
    }

    @inline(__always)
    private func withLock<T>(_ body: () -> T) -> T {
        os_unfair_lock_lock(lock)
        defer { os_unfair_lock_unlock(lock) }
        return body()
    }
}

extension AnyCancellable {
    func store(in bag: SubscriptionBag, for key: UUID) {
        bag.insert(self, for: key)
    }
}
