//
//  SubscriptionBagTest.swift
//  LittleBlueToothTests
//
//  Unit tests for the thread-safe SubscriptionBag. These exercise the
//  exact race condition that caused SIGSEGV in production: concurrent
//  mutation of a Dictionary<UUID, AnyCancellable> from multiple queues.
//
//  Verify thread safety with Thread Sanitizer:
//      swift test --sanitize=thread --filter SubscriptionBagTest
//

import XCTest
import Combine
@testable import LittleBlueToothForTest

final class SubscriptionBagTest: XCTestCase {

    // Concurrent inserts and removes across many queues — the exact
    // pattern that corrupted the Dictionary in production (store() on
    // caller queue + remove() on central manager queue via sinks).
    func testConcurrentInsertAndRemoveIsRaceFree() {
        let bag = SubscriptionBag()
        let iterations = 500
        let done = expectation(description: "done")
        done.expectedFulfillmentCount = iterations

        let queue = DispatchQueue(
            label: "test.subscriptionbag.insert-remove",
            attributes: .concurrent
        )

        for _ in 0..<iterations {
            queue.async {
                let key = UUID()
                let cancellable = AnyCancellable({})
                cancellable.store(in: bag, for: key)
                bag.remove(key)
                done.fulfill()
            }
        }

        wait(for: [done], timeout: 10)
    }

    // Insert/cancelAll interleaving — simulates a disconnect (cancelAll)
    // happening concurrently with in-flight GATT operations (inserts).
    func testConcurrentInsertsInterleavedWithCancelAllIsRaceFree() {
        let bag = SubscriptionBag()
        let iterations = 1000
        let done = expectation(description: "done")
        done.expectedFulfillmentCount = iterations

        let queue = DispatchQueue(
            label: "test.subscriptionbag.mixed",
            attributes: .concurrent
        )

        for i in 0..<iterations {
            if i.isMultiple(of: 2) {
                queue.async {
                    AnyCancellable({}).store(in: bag, for: UUID())
                    done.fulfill()
                }
            } else {
                queue.async {
                    bag.cancelAll()
                    done.fulfill()
                }
            }
        }

        wait(for: [done], timeout: 10)
    }

    func testRemoveCancelsTheSubscription() {
        let bag = SubscriptionBag()
        let cancelled = expectation(description: "cancelled")
        let key = UUID()
        AnyCancellable { cancelled.fulfill() }.store(in: bag, for: key)
        bag.remove(key)
        wait(for: [cancelled], timeout: 1)
    }

    func testCancelAllCancelsEverySubscription() {
        let bag = SubscriptionBag()
        let count = 25
        let cancelled = expectation(description: "all cancelled")
        cancelled.expectedFulfillmentCount = count

        for _ in 0..<count {
            AnyCancellable { cancelled.fulfill() }.store(in: bag, for: UUID())
        }
        bag.cancelAll()
        wait(for: [cancelled], timeout: 1)
    }

    func testDeinitCancelsOutstandingSubscriptions() {
        let cancelled = expectation(description: "deinit cancels")
        cancelled.expectedFulfillmentCount = 10

        autoreleasepool {
            let bag = SubscriptionBag()
            for _ in 0..<10 {
                AnyCancellable { cancelled.fulfill() }
                    .store(in: bag, for: UUID())
            }
            _ = bag
        }

        wait(for: [cancelled], timeout: 1)
    }
}
