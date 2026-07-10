use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;

const MAX_POOL_SIZE: usize = 1000;

const CITIES: &[&str] = &[
    "San Francisco",
    "Los Angeles",
    "New York",
    "Chicago",
    "Houston",
    "Seattle",
    "Boston",
    "Miami",
    "Denver",
    "Austin",
];

const STATES: &[&str] = &["CA", "NY", "IL", "TX", "WA", "MA", "FL", "CO", "OR", "GA"];

const ITEM_NAMES: &[&str] = &[
    "laptop",
    "bicycle",
    "backpack",
    "coffee maker",
    "desk lamp",
    "keyboard",
    "headphones",
    "shoes",
    "jacket",
    "watch",
];

const CHANNELS: &[&str] = &[
    "Google",
    "Facebook",
    "Twitter",
    "Direct",
    "Affiliate",
    "Referral",
    "Email",
    "Organic",
];

const DOMAINS: &[&str] = &["example.com", "test.com", "demo.org", "mysite.net"];

#[derive(Debug, Clone, PartialEq)]
pub struct Person {
    pub id: u64,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: u64,
    pub extra: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Auction {
    pub id: u64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: u64,
    pub reserve: u64,
    pub date_time: u64,
    pub expires: u64,
    pub seller: u64,
    pub category: u64,
    pub extra: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bid {
    pub auction: u64,
    pub bidder: u64,
    pub price: u64,
    pub channel: String,
    pub url: String,
    pub date_time: u64,
    pub extra: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NexmarkEvent {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

fn escape_sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

impl NexmarkEvent {
    pub fn to_insert_sql(&self) -> String {
        match self {
            NexmarkEvent::Person(p) => {
                format!(
                    "INSERT INTO person (id, name, email_address, credit_card, city, state, date_time, extra) \
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                    p.id,
                    escape_sql_string(&p.name),
                    escape_sql_string(&p.email_address),
                    escape_sql_string(&p.credit_card),
                    escape_sql_string(&p.city),
                    escape_sql_string(&p.state),
                    p.date_time,
                    escape_sql_string(&p.extra),
                )
            }
            NexmarkEvent::Auction(a) => {
                format!(
                    "INSERT INTO auction (id, item_name, description, initial_bid, reserve, date_time, expires, seller, category, extra) \
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                    a.id,
                    escape_sql_string(&a.item_name),
                    escape_sql_string(&a.description),
                    a.initial_bid,
                    a.reserve,
                    a.date_time,
                    a.expires,
                    a.seller,
                    a.category,
                    escape_sql_string(&a.extra),
                )
            }
            NexmarkEvent::Bid(b) => {
                format!(
                    "INSERT INTO bid (auction, bidder, price, channel, url, date_time, extra) \
                     VALUES ({}, {}, {}, {}, {}, {}, {})",
                    b.auction,
                    b.bidder,
                    b.price,
                    escape_sql_string(&b.channel),
                    escape_sql_string(&b.url),
                    b.date_time,
                    escape_sql_string(&b.extra),
                )
            }
        }
    }
}

pub struct NexmarkGenerator {
    rng: StdRng,
    event_count: u64,
    person_id_pool: VecDeque<u64>,
    auction_id_pool: VecDeque<u64>,
    next_person_id: u64,
    next_auction_id: u64,
    current_time: u64,
}

impl NexmarkGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            event_count: 0,
            person_id_pool: VecDeque::with_capacity(MAX_POOL_SIZE),
            auction_id_pool: VecDeque::with_capacity(MAX_POOL_SIZE),
            next_person_id: 1000,
            next_auction_id: 1000,
            current_time: 1000000000,
        }
    }
}

impl Iterator for NexmarkGenerator {
    type Item = NexmarkEvent;

    fn next(&mut self) -> Option<Self::Item> {
        let time_delta = self.rng.gen_range(1..=20);
        self.current_time += time_delta;

        let event_type_num = self.event_count % 50;
        self.event_count += 1;

        if event_type_num == 0 {
            // Generate Person
            let id = self.next_person_id;
            self.next_person_id += 1;

            let name = format!("Person {id}");
            let email_address = format!(
                "person_{}@{}",
                id,
                DOMAINS[self.rng.gen_range(0..DOMAINS.len())]
            );
            let credit_card = {
                let mut card = String::new();
                for _ in 0..4 {
                    card.push_str(&format!("{:04}", self.rng.gen_range(0..10000)));
                }
                card
            };
            let city = CITIES[self.rng.gen_range(0..CITIES.len())].to_string();
            let state = STATES[self.rng.gen_range(0..STATES.len())].to_string();
            let extra = format!("extra info for person {id}");

            let person = Person {
                id,
                name,
                email_address,
                credit_card,
                city,
                state,
                date_time: self.current_time,
                extra,
            };

            if self.person_id_pool.len() >= MAX_POOL_SIZE {
                self.person_id_pool.pop_front();
            }
            self.person_id_pool.push_back(id);

            Some(NexmarkEvent::Person(person))
        } else if (1..=3).contains(&event_type_num) {
            // Generate Auction
            let id = self.next_auction_id;
            self.next_auction_id += 1;

            let item_name = format!(
                "{} {}",
                ITEM_NAMES[self.rng.gen_range(0..ITEM_NAMES.len())],
                id
            );
            let description = format!("A high quality {item_name} up for auction");
            let initial_bid = self.rng.gen_range(10..1000);
            let reserve = initial_bid + self.rng.gen_range(50..500);
            let expires = self.current_time + self.rng.gen_range(60_000..600_000);

            let seller = if self.person_id_pool.is_empty() {
                self.next_person_id
            } else {
                let idx = self.rng.gen_range(0..self.person_id_pool.len());
                self.person_id_pool[idx]
            };

            let category = self.rng.gen_range(1..=10);
            let extra = format!("auction extra {id}");

            let auction = Auction {
                id,
                item_name,
                description,
                initial_bid,
                reserve,
                date_time: self.current_time,
                expires,
                seller,
                category,
                extra,
            };

            if self.auction_id_pool.len() >= MAX_POOL_SIZE {
                self.auction_id_pool.pop_front();
            }
            self.auction_id_pool.push_back(id);

            Some(NexmarkEvent::Auction(auction))
        } else {
            // Generate Bid
            let auction = if self.auction_id_pool.is_empty() {
                self.next_auction_id
            } else {
                let idx = self.rng.gen_range(0..self.auction_id_pool.len());
                self.auction_id_pool[idx]
            };

            let bidder = if self.person_id_pool.is_empty() {
                self.next_person_id
            } else {
                let idx = self.rng.gen_range(0..self.person_id_pool.len());
                self.person_id_pool[idx]
            };

            let price = self.rng.gen_range(100..5000);
            let channel = CHANNELS[self.rng.gen_range(0..CHANNELS.len())].to_string();
            let url = format!("http://example.com/auction/{auction}");
            let extra = format!("bid extra info for {auction}");

            let bid = Bid {
                auction,
                bidder,
                price,
                channel,
                url,
                date_time: self.current_time,
                extra,
            };

            Some(NexmarkEvent::Bid(bid))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nexmark_generator_determinism() {
        let mut gen1 = NexmarkGenerator::new(42);
        let mut gen2 = NexmarkGenerator::new(42);

        for _ in 0..1000 {
            let e1 = gen1.next().unwrap();
            let e2 = gen2.next().unwrap();
            assert_eq!(e1, e2);
        }
    }

    #[test]
    fn test_nexmark_generator_distribution() {
        let mut gen = NexmarkGenerator::new(12345);
        let mut persons = 0;
        let mut auctions = 0;
        let mut bids = 0;

        for _ in 0..10000 {
            match gen.next().unwrap() {
                NexmarkEvent::Person(_) => persons += 1,
                NexmarkEvent::Auction(_) => auctions += 1,
                NexmarkEvent::Bid(_) => bids += 1,
            }
        }

        assert_eq!(persons, 200); // Exactly 2%
        assert_eq!(auctions, 600); // Exactly 6%
        assert_eq!(bids, 9200); // Exactly 92%
    }
}
