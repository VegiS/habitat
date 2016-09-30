// Copyright (c) 2016 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::mpsc;
use std::sync::atomic::Ordering;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use protobuf;

use message::swim::{Swim, Swim_Type};
use server::{Server, outbound};
use member::{Member, Health};

pub struct Inbound<'a> {
    pub server: &'a Server,
    pub tx_outbound: mpsc::Sender<(SocketAddr, Swim)>,
}

impl<'a> Inbound<'a> {
    pub fn new(server: &'a Server, tx_outbound: mpsc::Sender<(SocketAddr, Swim)>) -> Inbound {
        Inbound {
            server: server,
            tx_outbound: tx_outbound,
        }
    }

    pub fn run(&self) {
        let mut recv_buffer: Vec<u8> = vec![0; 1024];
        loop {
            if self.server.pause.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            match self.server.socket.recv_from(&mut recv_buffer[..]) {
                Ok((length, addr)) => {
                    if self.server.check_blacklist(&addr) {
                        debug!("Not processing message from {} - it is blacklisted", addr);
                        continue;
                    }
                    let msg: Swim = match protobuf::parse_from_bytes(&recv_buffer[0..length]) {
                        Ok(msg) => msg,
                        Err(e) => {
                            // NOTE: In the future, we might want to blacklist people who send us
                            // garbage all the time.
                            error!("Error parsing protobuf: {:?}", e);
                            continue;
                        }
                    };
                    debug!("SWIM Message: {:?}", msg);
                    match msg.get_field_type() {
                        Swim_Type::PING => {
                            self.process_ping(addr, msg);
                        }
                        Swim_Type::ACK => {
                            self.process_ack(addr, msg);
                        }
                        Swim_Type::PINGREQ => {
                            self.process_pingreq(addr, msg);
                        }
                    }
                }
                Err(e) => {
                    match e.raw_os_error() {
                        Some(35) => {
                            // This is the normal non-blocking result
                        }
                        Some(_) => {
                            error!("UDP Receive error: {}", e);
                            debug!("UDP Receive error debug: {:?}", e);
                        }
                        None => {
                            error!("UDP Receive error: {}", e);
                        }
                    }
                }
            }
        }
    }

    fn process_pingreq(&self, addr: SocketAddr, mut msg: Swim) {
        trace_swim!(&self.server,
                    "recv-pingreq",
                    &format!("{}", addr),
                    Some(&msg));
        let ml = match self.server.member_list.read() {
            Ok(ml) => ml,
            Err(e) => panic!("The member list lock is poisoned: {:?}", e),
        };
        let target = match ml.get(msg.get_pingreq().get_target().get_id()) {
            Some(member) => member,
            None => {
                error!("PingReq request {:?} for invalid target", msg);
                return;
            }
        };
        // Set the route-back address to the one we received the pingreq from
        let mut from = msg.mut_pingreq().take_from();
        from.set_address(format!("{}", addr));
        outbound::ping(self.server,
                       target,
                       target.socket_address(),
                       Some(from.into()));

    }

    fn process_ack(&self, addr: SocketAddr, mut msg: Swim) {
        trace_swim!(&self.server, "recv-ack", &format!("{}", addr), Some(&msg));
        info!("Ack from {}@{}", msg.get_ack().get_from().get_id(), addr);
        if msg.get_ack().has_forward_to() {
            let me = match self.server.member.read() {
                Ok(me) => me,
                Err(e) => panic!("Member lock is poisoned: {:?}", e),
            };
            if me.get_id() != msg.get_ack().get_forward_to().get_id() {
                let forward_to_addr = match msg.get_ack().get_forward_to().get_address().parse() {
                    Ok(addr) => addr,
                    Err(e) => {
                        error!("Abandoning Ack forward: cannot parse member address: {}, {}",
                               msg.get_ack().get_forward_to().get_address(),
                               e);
                        return;
                    }
                };
                info!("Forwarding Ack from {}@{} to {}@{}",
                      msg.get_ack().get_from().get_id(),
                      addr,
                      msg.get_ack().get_forward_to().get_id(),
                      msg.get_ack().get_forward_to().get_address(),
                      );
                msg.mut_ack().mut_from().set_address(format!("{}", addr));
                outbound::forward_ack(self.server, forward_to_addr, msg);
                return;
            }
        }
        let membership = {
            let membership: Vec<(Member, Health)> = msg.take_membership()
                .iter()
                .map(|m| (Member::from(m.get_member()), Health::from(m.get_health())))
                .collect();
            membership
        };
        match self.tx_outbound.send((addr, msg)) {
            Ok(()) => {}
            Err(e) => panic!("Outbound thread has died - this shouldn't happen: #{:?}", e),
        }
        self.server.insert_from_rumors(membership);
    }

    fn process_ping(&self, addr: SocketAddr, mut msg: Swim) {
        trace_swim!(&self.server, "recv-ping", &format!("{}", addr), Some(&msg));
        let target: Member = msg.get_ping().get_from().into();
        if msg.get_ping().has_forward_to() {
            outbound::ack(self.server,
                          &target,
                          addr,
                          Some(msg.mut_ping().take_forward_to().into()));
        } else {
            outbound::ack(self.server, &target, addr, None);
        }
        // Populate the member for this sender with its remote address
        let from = {
            let mut ping = msg.mut_ping();
            let mut from = ping.take_from();
            from.set_address(format!("{}", addr));
            from
        };
        info!("Ping from {}@{}", from.get_id(), addr);
        self.server.insert_member(from.into(), Health::Alive);
        let membership: Vec<(Member, Health)> = msg.take_membership()
            .iter()
            .map(|m| (Member::from(m.get_member()), Health::from(m.get_health())))
            .collect();
        self.server.insert_from_rumors(membership);
    }
}
