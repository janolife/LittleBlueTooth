//
//  Helper.swift
//  LittleBlueTooth
//
//  Created by Andrea Finollo on 09/08/2020.
//

import Foundation
import Combine
#if TEST
import CoreBluetoothMock
#else
import CoreBluetooth
#endif

extension AnyCancellable {
  func store(in dictionary: inout [UUID : AnyCancellable],
             for key: UUID) {
    dictionary[key] = self
  }
}
extension Publisher {
    /// Republishes elements sent by the most recently received publisher.
   func flatMapLatest<T: Publisher>(_ transform: @escaping (Self.Output) -> T) -> AnyPublisher<T.Output, T.Failure> where T.Failure == Self.Failure {
       return map(transform).switchToLatest().eraseToAnyPublisher()
   }
}

extension TimeInterval {
    /// Get a `DispatchTimeInterval` from a TimeInterval.
    public var dispatchInterval: DispatchTimeInterval {
        let microseconds = Int64(self * TimeInterval(USEC_PER_SEC))
        return microseconds < Int.max ? DispatchTimeInterval.microseconds(Int(microseconds)) : DispatchTimeInterval.seconds(Int(self))
    }
}

#if TEST
extension CBMPeripheral {
    public var description: String {
        return "Test peripheral"
    }
}
#endif
