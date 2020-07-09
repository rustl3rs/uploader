// See: https://raw.githubusercontent.com/srijs/rust-tokio-retry/master/src/strategy/fibonacci_backoff.rs
// This has been adapted to work with futures_retry, which offers no out of the box strategies.
// base example: https://gitlab.com/mexus/futures-retry/-/blob/d3ba6a80578ebf5a2a996664902d18e048cd55f5/examples/tcp-client-complex.rs

// Licence from https://github.com/srijs/rust-tokio-retry/blob/af978d511c541909f8d7b3887f7002b5aeff47fe/LICENSE

// MIT License
//
// Copyright (c) 2017 Sam Rijs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::iter::Iterator;
use std::time::Duration;
use std::u64::MAX as U64_MAX;

/// A retry strategy driven by the fibonacci series.
///
/// Each retry uses a delay which is the sum of the two previous delays.
///
/// Depending on the problem at hand, a fibonacci retry strategy might
/// perform better and lead to better throughput than the `ExponentialBackoff`
/// strategy.
///
/// See ["A Performance Comparison of Different Backoff Algorithms under Different Rebroadcast Probabilities for MANETs."](http://www.comp.leeds.ac.uk/ukpew09/papers/12.pdf)
/// for more details.
#[derive(Debug, Clone)]
pub struct FibonacciBackoff {
    curr: u64,
    next: u64,
    max_delay: Option<Duration>,
    pub max_retries: Option<u64>,
    pub retries: u64,
}

impl FibonacciBackoff {
    /// Constructs a new fibonacci back-off strategy,
    /// given a base duration in milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self {
            curr: millis,
            next: millis,
            max_delay: None,
            max_retries: None,
            retries: 0,
        }
    }

    /// Apply a maximum delay. No retry delay will be longer than this `Duration`.
    pub fn max_delay(mut self, duration: Duration) -> Self {
        self.max_delay = Some(duration);
        self
    }

    /// Apply a maximum number of times to retry. Default is
    pub fn max_retries(mut self, max_retries: u64) -> Self {
        self.max_retries = Some(max_retries);
        self
    }
}

impl Iterator for FibonacciBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        // We have to stop retrying at some point.
        match self.max_retries {
            Some(max) => {
                if self.retries >= max {
                    return None;
                }
            }
            None => {
                if self.retries == u64::max_value() {
                    return None;
                }
            }
        }

        // at this point, we've retried, so increment the retry counter.
        self.retries += 1;

        let duration = Duration::from_millis(self.curr);

        // check if we reached max delay
        if let Some(ref max_delay) = self.max_delay {
            if duration > *max_delay {
                return Some(*max_delay);
            }
        }

        if let Some(next_next) = self.curr.checked_add(self.next) {
            self.curr = self.next;
            self.next = next_next;
        } else {
            self.curr = self.next;
            self.next = U64_MAX;
        }

        Some(duration)
    }
}

#[test]
fn returns_the_fibonacci_series_starting_at_10() {
    let mut iter = FibonacciBackoff::from_millis(10);
    assert_eq!(iter.next(), Some(Duration::from_millis(10)));
    assert_eq!(iter.next(), Some(Duration::from_millis(10)));
    assert_eq!(iter.next(), Some(Duration::from_millis(20)));
    assert_eq!(iter.next(), Some(Duration::from_millis(30)));
    assert_eq!(iter.next(), Some(Duration::from_millis(50)));
    assert_eq!(iter.next(), Some(Duration::from_millis(80)));
}

#[test]
fn saturates_at_maximum_value() {
    let mut iter = FibonacciBackoff::from_millis(U64_MAX);
    assert_eq!(iter.next(), Some(Duration::from_millis(U64_MAX)));
    assert_eq!(iter.next(), Some(Duration::from_millis(U64_MAX)));
}

#[test]
fn stops_increasing_at_max_delay() {
    let mut iter = FibonacciBackoff::from_millis(10).max_delay(Duration::from_millis(50));
    assert_eq!(iter.next(), Some(Duration::from_millis(10)));
    assert_eq!(iter.next(), Some(Duration::from_millis(10)));
    assert_eq!(iter.next(), Some(Duration::from_millis(20)));
    assert_eq!(iter.next(), Some(Duration::from_millis(30)));
    assert_eq!(iter.next(), Some(Duration::from_millis(50)));
    assert_eq!(iter.next(), Some(Duration::from_millis(50)));
}

#[test]
fn returns_max_when_max_less_than_base() {
    let mut iter = FibonacciBackoff::from_millis(20).max_delay(Duration::from_millis(10));

    assert_eq!(iter.next(), Some(Duration::from_millis(10)));
    assert_eq!(iter.next(), Some(Duration::from_millis(10)));
}
