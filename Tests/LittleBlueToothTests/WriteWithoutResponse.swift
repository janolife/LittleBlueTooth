//
//  WriteWithoutResponse.swift
//  LittleBlueToothTests
//
//  Created by Andrea Finollo on 28/07/2020.
//

import XCTest
import CoreBluetoothMock
import Combine
@testable import LittleBlueToothForTest

class WriteWithoutResponse: LittleBlueToothTests {

    override func setUp() async throws {
        try await super.setUp()
        var lttlCon = LittleBluetoothConfiguration()
        lttlCon.isLogEnabled = true
        littleBT = LittleBlueTooth(with: lttlCon)
    }

   func testWriteWOResponse() {
       disposeBag.removeAll()
       blinky.simulateProximityChange(.outOfRange)
       blinkyWOR.simulateProximityChange(.immediate)
       let charateristic = LittleBlueToothCharacteristic(characteristic: CBMUUID.ledCharacteristic.uuidString, for: CBMUUID.nordicBlinkyService.uuidString, properties: [.notify, .read, .write])
       let writeWOResp = expectation(description: "Write without response expectation")

       var data = Data()
       (0..<23).forEach { (val) in
           data.append(val)
       }
       littleBT.startDiscovery(withServices: nil, options: [CBCentralManagerScanOptionAllowDuplicatesKey : false])
       .flatMap { discovery in
           self.littleBT.connect(to: discovery)
       }
       .flatMap { _ in
           self.littleBT.write(to: charateristic, value: data, response: false)
       }
       .sink(receiveCompletion: { completion in
           print("Completion \(completion)")
       }) { (answer) in
           print("Answer \(answer)")
           self.littleBT.disconnect().sink(receiveCompletion: {_ in
           }) { (_) in
               writeWOResp.fulfill()
           }
           .store(in: &self.disposeBag)

       }
       .store(in: &disposeBag)
        waitForExpectations(timeout: 10)
   }
    
    func testWriteWOResponseMoreBuffer() {
        disposeBag.removeAll()
        blinky.simulateProximityChange(.outOfRange)
        blinkyWOR.simulateProximityChange(.immediate)
        let charateristic = LittleBlueToothCharacteristic(characteristic: CBMUUID.ledCharacteristic.uuidString, for: CBMUUID.nordicBlinkyService.uuidString, properties: [.notify, .read, .write])
        let writeWOResp = expectation(description: "Write without response expectation")

        var data = Data()
        (0..<30).forEach { (val) in
            data.append(val)
        }
        littleBT.startDiscovery(withServices: nil, options: [CBCentralManagerScanOptionAllowDuplicatesKey : false])
        .flatMap { discovery in
            self.littleBT.connect(to: discovery)
        }
        .flatMap { _ in
            self.littleBT.write(to: charateristic, value: data, response: false)
        }
        .sink(receiveCompletion: { completion in
            print("Completion \(completion)")
        }) { (answer) in
            print("Answer \(answer)")
            self.littleBT.disconnect().sink(receiveCompletion: {_ in
            }) { (_) in
                writeWOResp.fulfill()
            }
            .store(in: &self.disposeBag)

        }
        .store(in: &disposeBag)
         waitForExpectations(timeout: 10)
    }


}
