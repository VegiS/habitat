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

use std::collections::HashMap;
use std::fmt;
use std::iter::IntoIterator;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use uuid::Uuid;
use rand::{thread_rng, Rng};

use rumor::RumorKey;
use message::swim::{Member as ProtoMember, Membership as ProtoMembership,
                    Membership_Health as ProtoMembership_Health};

const PINGREQ_TARGETS: usize = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Health {
    Alive,
    Suspect,
    Confirmed,
}

impl From<ProtoMembership_Health> for Health {
    fn from(pm_health: ProtoMembership_Health) -> Health {
        match pm_health {
            ProtoMembership_Health::ALIVE => Health::Alive,
            ProtoMembership_Health::SUSPECT => Health::Suspect,
            ProtoMembership_Health::CONFIRMED => Health::Confirmed,
        }
    }
}

impl fmt::Display for Health {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Health::Alive => write!(f, "Alive"),
            &Health::Suspect => write!(f, "Suspect"),
            &Health::Confirmed => write!(f, "Confirmed"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    pub proto: ProtoMember,
}

impl Member {
    pub fn new() -> Member {
        let mut proto_member = ProtoMember::new();
        proto_member.set_id(Uuid::new_v4().simple().to_string());
        proto_member.set_incarnation(0);
        Member { proto: proto_member }
    }

    pub fn socket_address(&self) -> SocketAddr {
        match self.get_address().parse() {
            Ok(addr) => addr,
            Err(e) => {
                panic!("Cannot parse member {:?} address: {}", self, e);
            }
        }
    }
}

impl Deref for Member {
    type Target = ProtoMember;

    fn deref(&self) -> &ProtoMember {
        &self.proto
    }
}

impl DerefMut for Member {
    fn deref_mut(&mut self) -> &mut ProtoMember {
        &mut self.proto
    }
}

impl From<ProtoMember> for Member {
    fn from(member: ProtoMember) -> Member {
        Member { proto: member }
    }
}

impl<'a> From<&'a ProtoMember> for Member {
    fn from(member: &'a ProtoMember) -> Member {
        Member { proto: member.clone() }
    }
}

impl From<SocketAddr> for Member {
    fn from(socket: SocketAddr) -> Member {
        let mut member = Member::new();
        member.set_address(format!("{}", socket));
        member
    }
}

impl From<Member> for RumorKey {
    fn from(member: Member) -> RumorKey {
        RumorKey::new("member", member.get_id())
    }
}

impl<'a> From<&'a Member> for RumorKey {
    fn from(member: &'a Member) -> RumorKey {
        RumorKey::new("member", member.get_id())
    }
}

impl<'a> From<&'a &'a Member> for RumorKey {
    fn from(member: &'a &'a Member) -> RumorKey {
        RumorKey::new("member", member.get_id())
    }
}

// This is a Uuid type turned to a string
pub type UuidSimple = String;

#[derive(Debug, Clone)]
pub struct MemberList {
    members: HashMap<UuidSimple, Member>,
    health: HashMap<UuidSimple, Health>,
}

impl MemberList {
    pub fn new() -> MemberList {
        MemberList {
            members: HashMap::new(),
            health: HashMap::new(),
        }
    }

    pub fn insert(&mut self, member: Member, health: Health) -> bool {
        let share_rumor: bool;
        // If we have an existing member record..
        if let Some(current_member) = self.members.get(member.get_id()) {
            // If my incarnation is newer than the member we are being asked
            // to insert, we want to prefer our member, health and all.
            if current_member.get_incarnation() > member.get_incarnation() {
                share_rumor = false;
                // If the new rumor has a higher incarnation than our status, we want
                // to prefer it.
            } else if member.get_incarnation() > current_member.get_incarnation() {
                share_rumor = true;
            } else {
                // We know we have a health if we have a record
                let current_health = self.health_of(&current_member)
                    .expect("No health for a membership record should be impossible - did you \
                             not use insert?");
                // If curently healthy and the rumor is suspicion, then we are now suspicious.
                if *current_health == Health::Alive && health == Health::Suspect {
                    share_rumor = true;
                    // If currently healthy and the rumor is confirmation, then we are now confirmed
                } else if *current_health == Health::Alive && health == Health::Confirmed {
                    share_rumor = true;
                    // If we are both alive, then nothing to see here.
                } else if *current_health == Health::Alive && health == Health::Alive {
                    share_rumor = false;
                    // If currently suspicous and the rumor is alive, then we are still suspicious
                } else if *current_health == Health::Suspect && health == Health::Alive {
                    share_rumor = false;
                    // If currently suspicous and the rumor is suspicion, then nothing to see here.
                } else if *current_health == Health::Suspect && health == Health::Suspect {
                    share_rumor = false;
                    // If currently suspicious and the rumor is confirmation, then we are now confirmed
                } else if *current_health == Health::Suspect && health == Health::Confirmed {
                    share_rumor = true;
                    // When we are currently confirmed, we stay that way until something with a
                    // higher incarnation changes our mind.
                } else {
                    share_rumor = false;
                }
            }
        } else {
            share_rumor = true;
        }
        if share_rumor == true {
            self.health.insert(String::from(member.get_id()), health);
            self.members.insert(String::from(member.get_id()), member);
        }
        share_rumor
    }

    pub fn health_of(&self, member: &Member) -> Option<&Health> {
        self.health.get(member.get_id())
    }

    pub fn insert_health(&mut self, member: &Member, health: Health) -> bool {
        if let Some(current_health) = self.health.get(member.get_id()) {
            if *current_health == health {
                return false;
            }
        }
        self.health.insert(String::from(member.get_id()), health);
        true
    }

    pub fn membership_for(&self, member_id: &str) -> ProtoMembership {
        let mut pm = ProtoMembership::new();
        let health = self.health
            .get(member_id)
            .expect("Should have membership before calling membership_for");
        let mhealth = match health {
            &Health::Alive => ProtoMembership_Health::ALIVE,
            &Health::Suspect => ProtoMembership_Health::SUSPECT,
            &Health::Confirmed => ProtoMembership_Health::CONFIRMED,
        };
        let member = self.get(member_id)
            .expect("Should have membership before calling membership_for");
        pm.set_health(mhealth);
        pm.set_member(member.proto.clone());
        pm
    }

    pub fn members(&self) -> Vec<&Member> {
        self.members.values().collect()
    }

    pub fn check_list(&self, exclude_id: &str) -> Vec<Member> {
        let mut members: Vec<Member> =
            self.members.values().filter(|v| v.get_id() != exclude_id).map(|v| v.clone()).collect();
        let mut rng = thread_rng();
        rng.shuffle(&mut members);
        members
    }

    pub fn pingreq_targets(&self, sending_member: &Member, target_member: &Member) -> Vec<&Member> {
        let mut members = self.members();
        let mut rng = thread_rng();
        rng.shuffle(&mut members);
        members.into_iter()
            .filter(|m| {
                m.get_id() != sending_member.get_id() && m.get_id() != target_member.get_id()
            })
            .take(PINGREQ_TARGETS)
            .collect()
    }
}

impl Deref for MemberList {
    type Target = HashMap<UuidSimple, Member>;

    fn deref(&self) -> &HashMap<UuidSimple, Member> {
        &self.members
    }
}

#[cfg(test)]
mod tests {
    mod member {
        use uuid::Uuid;
        use message::swim;
        use member::Member;

        // Sets the uuid to simple, and the incarnation to zero.
        #[test]
        fn new() {
            let member = Member::new();
            assert_eq!(member.proto.get_id().len(), 32);
            assert_eq!(member.proto.get_incarnation(), 0);
        }

        // Takes a member in from a protobuf
        #[test]
        fn new_from_proto() {
            let mut proto = swim::Member::new();
            let uuid = Uuid::new_v4();
            proto.set_id(uuid.simple().to_string());
            proto.set_incarnation(0);
            let proto2 = proto.clone();
            let member: Member = proto.into();
            assert_eq!(proto2, member.proto);
        }
    }

    mod member_list {
        use member::{Member, MemberList, Health, PINGREQ_TARGETS};

        fn populated_member_list(size: u64) -> MemberList {
            let mut ml = MemberList::new();
            for _x in 0..size {
                let m = Member::new();
                ml.insert(m, Health::Alive);
            }
            ml
        }

        #[test]
        fn new() {
            let ml = MemberList::new();
            assert_eq!(ml.len(), 0);
        }

        #[test]
        fn insert() {
            let ml = populated_member_list(4);
            assert_eq!(ml.len(), 4);
        }

        #[test]
        fn check_list() {
            let ml = populated_member_list(1000);
            let list_a = ml.check_list("foo");
            let list_b = ml.check_list("foo");
            assert!(list_a != list_b);
        }

        #[test]
        fn health_of() {
            let ml = populated_member_list(1);
            for member in ml.members() {
                assert_eq!(ml.health_of(member), Some(&Health::Alive));
            }
        }

        #[test]
        fn pingreq_targets() {
            let ml = populated_member_list(10);
            let members = ml.members();
            let from: &Member = members.get(0).unwrap();
            let target: &Member = members.get(1).unwrap();
            assert_eq!(ml.pingreq_targets(from, target).len(), PINGREQ_TARGETS);
        }

        #[test]
        fn pingreq_targets_excludes_pinging_member() {
            let ml = populated_member_list(3);
            let members = ml.members();
            let from: &Member = members.get(0).unwrap();
            let target: &Member = members.get(1).unwrap();
            let targets = ml.pingreq_targets(from, target);
            assert_eq!(targets.iter().find(|&&x| x.get_id() == from.get_id()).is_none(),
                       true);
        }

        #[test]
        fn pingreq_targets_excludes_target_member() {
            let ml = populated_member_list(3);
            let members = ml.members();
            let from: &Member = members.get(0).unwrap();
            let target: &Member = members.get(1).unwrap();
            let targets = ml.pingreq_targets(from, target);
            assert_eq!(targets.iter().find(|&&x| x.get_id() == target.get_id()).is_none(),
                       true);
        }

        #[test]
        fn pingreq_targets_minimum_viable_pingreq_size_is_three() {
            let ml = populated_member_list(3);
            let members = ml.members();
            let from: &Member = members.get(0).unwrap();
            let target: &Member = members.get(1).unwrap();
            let targets = ml.pingreq_targets(from, target);
            assert_eq!(targets.len(), 1);
        }

        #[test]
        fn insert_no_member() {
            let mut ml = MemberList::new();
            let member = Member::new();
            let mcheck = member.clone();
            assert_eq!(ml.insert(member, Health::Alive), true);
            assert_eq!(ml.health_of(&mcheck).unwrap(), &Health::Alive);
        }

        #[test]
        fn insert_existing_member_lower_incarnation() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let mut member_two = member_one.clone();
            member_two.set_incarnation(1);
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Alive), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Alive);

            assert_eq!(ml.insert(member_two, Health::Alive), true);
            assert_eq!(ml.get(mcheck_two.get_id()).unwrap().get_incarnation(), 1);
        }

        #[test]
        fn insert_existing_member_higher_incarnation() {
            let mut ml = MemberList::new();
            let mut member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            member_one.set_incarnation(1);

            assert_eq!(ml.insert(member_one, Health::Alive), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Alive);

            assert_eq!(ml.insert(member_two, Health::Alive), false);
            assert_eq!(ml.get(mcheck_two.get_id()).unwrap().get_incarnation(), 1);
        }

        #[test]
        fn insert_equal_incarnation_current_alive_new_alive() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();

            assert_eq!(ml.insert(member_one, Health::Alive), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Alive);

            assert_eq!(ml.insert(member_two, Health::Alive), false);
        }

        #[test]
        fn insert_equal_incarnation_current_alive_new_suspect() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Alive), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Alive);

            assert_eq!(ml.insert(member_two, Health::Suspect), true);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Suspect);
        }

        #[test]
        fn insert_equal_incarnation_current_alive_new_confirmed() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Alive), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Alive);

            assert_eq!(ml.insert(member_two, Health::Confirmed), true);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Confirmed);
        }

        #[test]
        fn insert_equal_incarnation_current_suspect_new_alive() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Suspect), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Suspect);

            assert_eq!(ml.insert(member_two, Health::Alive), false);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Suspect);
        }

        #[test]
        fn insert_equal_incarnation_current_suspect_new_suspect() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Suspect), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Suspect);

            assert_eq!(ml.insert(member_two, Health::Suspect), false);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Suspect);
        }

        #[test]
        fn insert_equal_incarnation_current_suspect_new_confirmed() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Suspect), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Suspect);

            assert_eq!(ml.insert(member_two, Health::Confirmed), true);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Confirmed);
        }

        #[test]
        fn insert_equal_incarnation_current_confirmed_new_alive() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Confirmed), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Confirmed);

            assert_eq!(ml.insert(member_two, Health::Alive), false);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Confirmed);
        }

        #[test]
        fn insert_equal_incarnation_current_confirmed_new_suspect() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Confirmed), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Confirmed);

            assert_eq!(ml.insert(member_two, Health::Suspect), false);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Confirmed);
        }

        #[test]
        fn insert_equal_incarnation_current_confirmed_new_confirmed() {
            let mut ml = MemberList::new();
            let member_one = Member::new();
            let mcheck_one = member_one.clone();
            let member_two = member_one.clone();
            let mcheck_two = member_two.clone();

            assert_eq!(ml.insert(member_one, Health::Confirmed), true);
            assert_eq!(ml.health_of(&mcheck_one).unwrap(), &Health::Confirmed);

            assert_eq!(ml.insert(member_two, Health::Confirmed), false);
            assert_eq!(ml.health_of(&mcheck_two).unwrap(), &Health::Confirmed);
        }

    }
}
