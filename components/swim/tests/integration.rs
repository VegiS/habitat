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

extern crate env_logger;
extern crate time;
extern crate habitat_swim;

#[macro_use]
mod common;

use habitat_swim::member::Health;

#[test]
fn two_members_meshed_confirm_one_member() {
    let mut net = common::net::SwimNet::new(2);
    net.mesh();
    assert_wait_for_health_of!(net, 0, 1, Health::Alive);
    assert_wait_for_health_of!(net, 1, 0, Health::Alive);

    net[0].pause();
    assert_wait_for_health_of!(net, 1, 0, Health::Confirmed);
}

#[test]
fn six_members_meshed_confirm_one_member() {
    let mut net = common::net::SwimNet::new(6);
    net.mesh();
    net[0].pause();
    assert_wait_for_health_of!(net, 0, Health::Confirmed);
}

#[test]
fn six_members_meshed_partition_one_node_from_another_node_remains_alive() {
    let mut net = common::net::SwimNet::new(6);
    net.mesh();
    net.blacklist(0, 1);
    assert_wait_for_health_of!(net, 1, Health::Alive);
}

#[test]
fn six_members_meshed_partition_half_of_nodes_from_each_other_both_sides_confirmed() {
    let mut net = common::net::SwimNet::new(6);
    net.mesh();
    assert_wait_for_health_of!(net, 0, Health::Alive);
    net.partition(0..3, 3..6);
    assert_wait_for_health_of!(net, [0..3, 3..6], Health::Confirmed);
}

#[test]
fn six_members_unmeshed_become_fully_meshed_via_gossip() {
    let mut net = common::net::SwimNet::new(6);
    net.connect(0, 1);
    net.connect(1, 2);
    net.connect(2, 3);
    net.connect(3, 4);
    net.connect(4, 5);
    assert_wait_for_health_of!(net, [0..6, 0..6], Health::Alive);
}

#[test]
fn six_members_unmeshed_confirm_one_member() {
    let mut net = common::net::SwimNet::new(6);
    net.connect(0, 1);
    net.connect(1, 2);
    net.connect(2, 3);
    net.connect(3, 4);
    net.connect(4, 5);
    assert_wait_for_health_of!(net, [0..6, 0..6], Health::Alive);
    net[0].pause();
    assert_wait_for_health_of!(net, 0, Health::Confirmed);
}

#[test]
fn six_members_unmeshed_partition_and_rejoin_no_permanant_peers() {
    let mut net = common::net::SwimNet::new(6);
    net.connect(0, 1);
    net.connect(1, 2);
    net.connect(2, 3);
    net.connect(3, 4);
    net.connect(4, 5);
    assert_wait_for_health_of!(net, [0..6, 0..6], Health::Alive);
    net.partition(0..3, 3..6);
    assert_wait_for_health_of!(net, [0..3, 3..6], Health::Confirmed);
    net.unpartition(0..3, 3..6);
    net.wait_for_rounds(1);
    assert_wait_for_health_of!(net, [0..3, 3..6], Health::Confirmed);
}

#[test]
fn six_members_unmeshed_partition_and_rejoin_permanant_peers() {
    let mut net = common::net::SwimNet::new(6);
    net[0].member.write().expect("Member lock is poisoned").set_persistent(true);
    net[4].member.write().expect("Member lock is poisoned").set_persistent(true);
    net.connect(0, 1);
    net.connect(1, 2);
    net.connect(2, 3);
    net.connect(3, 4);
    net.connect(4, 5);
    assert_wait_for_health_of!(net, [0..6, 0..6], Health::Alive);
    net.partition(0..3, 3..6);
    assert_wait_for_health_of!(net, [0..3, 3..6], Health::Confirmed);
    net.unpartition(0..3, 3..6);
    net.wait_for_rounds(1);
    assert_wait_for_health_of!(net, [0..3, 3..6], Health::Alive);
}

#[test]
fn one_hundred_members_meshed_confirm_one_member() {
    let mut net = common::net::SwimNet::new(100);
    net.mesh();
    net[0].pause();
    assert_wait_for_health_of!(net, 0, Health::Confirmed);
}