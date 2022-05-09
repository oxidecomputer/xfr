//! An atomic ring for processing memory-mapped frames.
//!
//! This crate provides two primary data structures [`Ring`] and
//! [`FrameBuffer`]. Buffers hold an array of frames and rings provide
//! producer/consumer mechanics for implementing packet flows. The benefit of
//! this combination of data structures is that to implement packet flows, data
//! frames do not need to be moved or copied. Ring elements contain pointers to
//! frames and ring elements are what's moved around to implement packet flow.
//! This general design is heavily inspired by Linux's eXpress Data Path (XDP).
//! Xfr Rings are lock-free providing a higher-performance more robust substrate
//! than lock based rings.
//!
//! Basic packet flow
//! =================
//!
//! Let's start with a simple packet flow example. In this scenario we have two
//! data link interfaces. For each data link interface there is an external i/o
//! driver that is responsible for taking packets off the wire and placing them
//! in a buffer for further processing. The i/o driver is also responsible for
//! taking buffered packets that are ready for egress and putting them on the
//! wire. Packets transit between these two interfaces through a packet
//! processing engine.
//!
//! The interface i/o drivers and the packet processing engine interact through
//! a set of rings that provide access into an underlying frame buffer. In the
//! diagram below each interface is connected to an ingress ring (upper) that it
//! uses to buffer packets that are ready for further processing. Each interface
//! is also connected to an egress ring (lower) that it consumes packets from
//! that are ready to be put on the wire.
//!
//! ```text
//!                        h
//!                        t     r
//!     *------* produce  *=======================* consume  *--------*
//!     |      |--------->|0A|0B|0C|  |  |  |  |  |--------->|        |
//!     | ifx0 |          *=======================*          |        |
//!     |      |  consume *=======================*  produce |        |
//!     |      |<---------|  |  |  |  |  |  |  |  |<---------|        |
//!     *------*          *=======================*          |        |
//!                                                          |        |
//!                       *=======================*          |        |
//!                       |00|01|02|03|04|05|06|07|          |        |
//!                       *-----------------------*          |        |
//!                       |08|09|0A|0B|0C|0D|0E|0F|          | Packet |
//!                       *-----------------------*          | Engine |
//!                       |10|11|12|13|14|15|16|17|          |        |
//!                       *-----------------------*          |        |
//!                       |18|19|1A|1B|1C|1D|1E|1F|          |        |
//!                       *=======================*          |        |
//!                                                          |        |
//!     *------* produce  *=======================* consume  |        |
//!     |      |--------->|  |  |  |  |  |  |  |  |--------->|        |
//!     | ifx1 |          *=======================*          |        |
//!     |      |  consume *=======================*  produce |        |
//!     |      |<---------|  |  |  |  |  |  |  |  |<---------|        |
//!     *------*          *=======================*          *--------*
//! ```
//!
//!
//! The ingress and egress rings do not contain any packet frame data. Rather
//! they contain an index into a frame buffer that holds packets while they are
//! being processed. The act of packet processing happens over these
//! frame-pointer ring elements. This means we do not need to move packet data
//! around as it is processed.
//!
//! In the example above the producer has 3 frames to produce for processing.
//! The first step it must take is reserving 3 frame pointers from the ingress
//! ring buffer. This moves the `rsvd` index forward 3 positions. The interface
//! driver can now modify frame pointers between the `head` index and the `rsvd`
//! index. This is depicted in the diagram above by the `h` and `r` markers.
//!
//! Once the interface driver has a reserved region in the ingress ring buffer,
//! it must allocate frames from the frame buffer (depicted in the center of the
//! diagram). In this example the interface driver allocates 3 frames from the
//! frame buffer and it gets back frame addresses `0A`, `0B` and `0C`. It then
//! assigns these addresses to the frame pointer ring elements it has reserved.
//! This is also depicted in the diagram above.
//!
//! The interface driver can now write packets to the frame buffer through the
//! ring buffer pointers it owns. Once it has done that, it calls `produce(3)`
//! on the ingress ring. This results in the `head` index moving forward 3
//! positions.
//!
//! ```text
//!                             h
//!                       t     r
//!    *------* produce  *=======================* consume  *--------*
//!    |      |--------->|0A|0B|0C|  |  |  |  |  |--------->|        |
//!    | ifx0 |          *=======================*          | Packet |
//!    |      |  consume *=======================*  produce | Engine |
//!    |      |<---------|  |  |  |  |  |  |  |  |<---------|        |
//!    *------*          *=======================*          |  ...   |
//! ```
//!
//! This results in a space of 3 frame pointers between the tail depicted as the
//! marker `t` in the diagrams, and the head `h`. The space between these two
//! indices is the consumable space. This means that the packet engine now has
//! ownership of these frames e.g. the act of producing frames is transfer of
//! ownership from the producer to the consumer. When the packet engine has
//! finished processing the frames, it can call `consume(3)` to move the tail
//! index forward 3 frame pointers. At this point, assuming there have been no
//! new packets produced, there are no frames to produce and no frames to
//! consume on the ingress ring.
//!
//! ```text
//!                             t
//!                             h
//!                             r
//!    *------* produce  *=======================* consume  *--------*
//!    |      |--------->|0A|0B|0C|  |  |  |  |  |--------->|        |
//!    | ifx0 |          *=======================*          | Packet |
//!    |      |  consume *=======================*  produce | Engine |
//!    |      |<---------|  |  |  |  |  |  |  |  |<---------|        |
//!    *------*          *=======================*          |  ...   |
//! ```
//!
//! Now how about egress? The process works the same in reverse on the egress
//! ring. Here the packet engine is the producer and the interface driver is the
//! consumer.
//!
//! ```text
//!                             t
//!                             h
//!                             r
//!    *------* produce  *=======================* consume  *--------*
//!    |      |--------->|0A|0B|0C|  |  |  |  |  |--------->|        |
//!    | ifx0 |          *=======================*          | Packet |
//!    |      |  consume *=======================*  produce | Engine |
//!    |      |<---------|  |  |  |  |  |0C|0B|0A|<---------|        |
//!    *------*          *=======================*          |  ...   |
//!                                      r      h
//!                                             t
//! ```
//!
//! Here the packet engine starts by reserving 3 frame pointers on the egress
//! ring as depicted by the `r` marker in the diagram above. Once reserved it
//! writes the indices of the frames it has processed in those three frame
//! pointer elements within the ring. Then it moves the head pointer forward
//! with a `produce(3)` call
//!
//! ```text
//!                             t
//!                             h
//!                             r
//!    *------* produce  *=======================* consume  *--------*
//!    |      |--------->|0A|0B|0C|  |  |  |  |  |--------->|        |
//!    | ifx0 |          *=======================*          | Packet |
//!    |      |  consume *=======================*  produce | Engine |
//!    |      |<---------|  |  |  |  |  |0C|0B|0A|<---------|        |
//!    *------*          *=======================*          |  ...   |
//!                                      r      t
//!                                      h
//! ```
//!
//! The interface driver now owns the frames between the head and tail indices.
//! It takes the referenced frames and puts them on the wire. Then it calls
//! `consume(3)` resulting in the tail index catching up with head.
//!
//! ```text
//!                             t
//!                             h
//!                             r
//!    *------* produce  *=======================* consume  *--------*
//!    |      |--------->|0A|0B|0C|  |  |  |  |  |--------->|        |
//!    | ifx0 |          *=======================*          | Packet |
//!    |      |  consume *=======================*  produce | Engine |
//!    |      |<---------|  |  |  |  |  |0C|0B|0A|<---------|        |
//!    *------*          *=======================*          |  ...   |
//!                                      r
//!                                      h
//!                                      t
//! ```
//!
//! Tada! We've processed a pile of packets without copying any frame data. The
//! rings are a thread-safe mechanism for producers and consumers of packet
//! frames to exchange ownership of frames through pointers.
//!
//! Ring invariants
//! ===============
//!
//! The Ring data structure is able to work in a lock-free way because of a few
//! key invariants, combined with a notion of epochs. When an index rolls over
//! to the beginning of the ring it enters a new epoch of that ring. The
//! ring invariants can be summarized in the following inequality.
//!
//! ```text
//! t <= h <= r <= t+R
//! ```
//!
//! Taking each of thse in turn
//!
//! - `t <= h`: The tail can never eclipse the head.
//! - `h <= r`: The head can never eclipse the reserve.
//! - `r <= t+R`: The reserve can never eclipse the tail in the next epoch.
//!
//! The last constraint also insures that the indices span no more than two
//! epochs.
//!
//! When we want to compare two indices we need to know their reltaive position
//! within the ring e.g., the value modulo the size of the ring, as well as what
//! epoch the indices are in. Combining the facts that a) there is a complete
//! ordering across indices, b) and indices can only span a maximum of two
//! epochs - this allows us to track epochs as a single bit quantity. All we
//! need to know to compare two indicies is whether they are in the same or
//! different epochs. Which specific indices are being compared allows us to
//! determine which index is in the latter epoch. For example if we are
//! comparing `t` and `h` and they are in different epochs, we know that `h` is
//! in the later epoch because of the invariant `t` <= `h`.
//!
//! Epoch Encoding
//! ==============
//!
//! The single-bit representation of an epoch allows for a very efficient index
//! encoding scheme that can be read and written to atomically. Being able to
//! operate atomically on indices and their epochs is critically important for
//! achieving a lock free ring.
//!
//! Consider a ring consumer enforcing the invariant `t <= h`. Only the consumer
//! can move `t`, but a producer in another thread can move `h`. So if the
//! consumer reads `h` at time `a` and then reads the epoch for `h` at time `b`
//! but `h` moved between `a` and `b`, then the total representation of `h`
//! collected (its value and its epoch) is invalid. To get a consistent total
//! representation of `h` we would need to lock the index and epoch for `h`.
//!
//! However, because the epoch is just a one-bit quantity, we use the leading
//! bit of an index to encode an epoch. Doing this allows us to atomically load
//! the index and it's epoch. Ring indices are 64 bit values, so this
//! effectively makes them 63 bit values. I don't think we'll be in need of
//! Gangam style rings exceeding 9 quintillion elements any time soon.
//!
//! Usage
//! =====
//!
//! The following example demonstrates transferring 1.5 GB of data in 1500 byte
//! frames across a memory mapped ring.
//!
//! ```rust
//! use std::sync::Arc;
//! use std::thread::{spawn, sleep};
//! use std::time::Duration;
//! use std::str::from_utf8;
//!
//! use xfr::*;
//!
//! use rand::Rng;
//! // Transfer 1,000,000 1500 byte frames (1.5 GB)
//! const R: usize = 1024;
//! const N: usize = 4096;
//! const F: usize = 1500;
//! const K: u32 = 1000000;
//!
//! let fb = Arc::new(FrameBuffer::<N, F>::new());
//! let (p, c) = ring::<R, N, F>(fb);
//!
//! let t1 = spawn(move|| {
//! // create test data we're going to send the full 1500 bytes per
//! // packet, but only bother writing a 4 byte integer in each packet.
//! let mut data = Vec::new();
//! for i in 0..K {
//!     data.push(i.to_be_bytes());
//! }
//!
//! let mut rng = rand::thread_rng();
//! let mut i = 0;
//! loop {
//!     let count = usize::min(rng.gen_range(0..10), (K-i) as usize);
//!     let fps = match p.reserve(count) {
//!         Ok(fps) => fps,
//!         Err(_) => continue,
//!     };
//!     for fp in fps {
//!         p.write(fp, data[i as usize].as_slice());
//!         i += 1;
//!     }
//!     p.produce(count).unwrap();
//!     if i >= K {
//!         break;
//!     }
//! }
//! println!("producer finished");
//! });
//!
//! let t2 = spawn(move|| {
//! let mut total = 0u32;
//! loop {
//!     let mut count = 0;
//!     let consumable = c.consumable();
//!     for fp in consumable {
//!         let content = c.read(fp);
//!         let x = u32::from_be_bytes(content.try_into().unwrap());
//!         assert_eq!(x, total);
//!         total += 1;
//!         count += 1;
//!     }
//!     if count == 0 {
//!         continue
//!     }
//!     c.consume(count).unwrap();
//!     if total >= K {
//!         break;
//!     }
//! }
//! println!("consumer finished");
//! });
//!
//! t1.join().unwrap();
//! t2.join().unwrap();
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A frame buffer holds an array of frames and an index that identifies the
/// first allocatable frames. Frames are always allocated in a forward linear
/// fashion. There is no tracking of allocated frames. If your frame gets
/// overwritten, it's because you held on to it too long. Users of a frame
/// buffer should ensure that the buffer is sufficiently sized to allow external
/// holders of frames to do what they must with them before they are reallocated
/// e.g., frames come with an expiration of one ring epoch. This is essentially
/// a big linear ring of frames. The focus is on fast allocation. Allocations
/// against this frame are expected at rates in the millions to tens of millions
/// per second.
pub struct FrameBuffer<const N: usize, const F: usize> {
    index: AtomicU64,
    frames: UnsafeCell<Box<[Frame<F>]>>,
}

impl<const N: usize, const F: usize> FrameBuffer<N, F> {
    pub fn new() -> Self {
        Self {
            index: AtomicU64::new(0),
            frames: UnsafeCell::new(
                (0..N)
                    .map(|_| Frame::<F>::new())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
        }
    }

    pub fn alloc(&self, count: u64) -> FrameBufferIterator<N, F> {
        let index = self.index.fetch_add(count, Ordering::Relaxed) % (N as u64);
        FrameBufferIterator::<N, F> {
            buf: UnsafeCell::new(self),
            index,
            count,
        }
    }
}

unsafe impl<const N: usize, const F: usize> Send for FrameBuffer<N, F> {}
unsafe impl<const N: usize, const F: usize> Sync for FrameBuffer<N, F> {}

/// A Frame is a wrapper for a network data frame. The data inside is owned by
/// this frame.
pub struct Frame<const F: usize> {
    data: UnsafeCell<[u8; F]>,
    len: usize,
}

impl<const F: usize> Frame<F> {
    pub fn new() -> Self {
        Self {
            data: UnsafeCell::new([0u8; F]),
            len: 0,
        }
    }
}

/// An iterator over a set of frames. In the event that a series of frames rolls
/// over from the end of the buffer to the beginning this iterator handles that
/// seamlessly.
pub struct FrameBufferIterator<'a, const N: usize, const F: usize> {
    buf: UnsafeCell<&'a FrameBuffer<N, F>>,
    index: u64,
    count: u64,
}
impl<'a, const N: usize, const F: usize> Iterator for FrameBufferIterator<'a, N, F> {
    type Item = (usize, &'a Frame<F>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            None
        } else {
            let i = self.index as usize;
            self.count -= 1;
            self.index = (self.index + 1) % (N as u64);

            let frames = unsafe {
                let buf = &*self.buf.get();
                &*buf.frames.get()
            };
            Some((i, &frames[i]))
        }
    }
}

/// A Ring arbitrates access to a frame buffer. Rings are directional, there are
/// producers that allocate and modify and produce elements on one side of a
/// ring and consumers that read and consume frames on the other side of a ring.
/// A ring contains ring elements which are essentially a wrapper around a
/// FrameBuffer specific address.
///
/// Rings are not designed to be consumed directly by users of this crate. They
/// underpin [`RingConsumer`] and [`RingProducer`] objects. Similar to channels
/// these are always created in pairs by the [`ring`] function.
pub struct Ring<const R: usize, const N: usize, const F: usize> {
    buf: UnsafeCell<Arc<FrameBuffer<N, F>>>,
    elements: UnsafeCell<Box<[RingElement]>>,

    head: AtomicU64,
    tail: AtomicU64,
    rsvd: AtomicU64,
}

impl<'a, const R: usize, const N: usize, const F: usize> Ring<R, N, F> {
    pub fn new(fb: Arc<FrameBuffer<N, F>>) -> Self {
        Self {
            buf: UnsafeCell::new(fb),
            elements: UnsafeCell::new(vec![RingElement::default(); R].into_boxed_slice()),
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            rsvd: AtomicU64::new(0),
        }
    }
}

impl<'a, const R: usize, const N: usize, const F: usize> fmt::Debug for Ring<R, N, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(&format!("Ring<R={},N={},F={}>", R, N, F))
            .field("head", &self.head.load(Ordering::Relaxed))
            .field("tail", &self.tail.load(Ordering::Relaxed))
            .field("rsvd", &self.rsvd.load(Ordering::Relaxed))
            .finish()
    }
}

/// A wring element is a wrapper around an address to a [`Frame`] within a
/// [`FrameBuffer`].
#[derive(Debug, Copy, Clone, Default)]
pub struct RingElement {
    addr: usize,
}

/// The ring function creates a [`Ring`] object and returns a [`RingProducer`]
/// and [`RingConsumer`] instance for interacting with the [`Ring`].
pub fn ring<const R: usize, const N: usize, const F: usize>(
    buf: Arc<FrameBuffer<N, F>>,
) -> (RingProducer<R, N, F>, RingConsumer<R, N, F>) {
    let r = Arc::new(Ring::new(buf));

    (
        RingProducer::<R, N, F> {
            ring: UnsafeCell::new(r.clone()),
        },
        RingConsumer::<R, N, F> {
            ring: UnsafeCell::new(r.clone()),
        },
    )
}

/// An iterator over a set of [`RingElement`]s. In the event that a series of
/// elements rolls over from the end of the ring to the beginning, this iterator
/// handles that seamlessly.
pub struct RingIterator<const R: usize, const N: usize, const F: usize> {
    ring: UnsafeCell<Arc<Ring<R, N, F>>>,
    index: u64,
    count: u64,
}

impl<const R: usize, const N: usize, const F: usize> fmt::Debug for RingIterator<R, N, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(&format!("RingIterator<R={},N={},F={}>", R, N, F))
            .field("index", &self.index)
            .field("count", &self.count)
            .finish()
    }
}

impl<const R: usize, const N: usize, const F: usize> Iterator for RingIterator<R, N, F> {
    type Item = RingElement;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            None
        } else {
            let i = self.index as usize;
            //println!(">>> I={}", self.index);
            self.count -= 1;
            self.index = (self.index + 1) % (R as u64);
            //println!(">>> I={}", self.index);

            let elements = unsafe {
                let ring = &*self.ring.get();
                &*ring.elements.get()
            };
            Some(elements[i])
        }
    }
}

/// A [`RingProducer`] provides write access to a [`Ring`].
pub struct RingProducer<const R: usize, const N: usize, const F: usize> {
    ring: UnsafeCell<Arc<Ring<R, N, F>>>,
}

impl<const R: usize, const N: usize, const F: usize> RingProducer<R, N, F> {
    pub fn reserve(&self, count: usize) -> Result<RingIterator<R, N, F>, ()> {
        let ring = unsafe { &mut *self.ring.get() };

        // Enforce ring invariant r < t+R
        let (t_epoch, t) = read_index(ring.tail.load(Ordering::Relaxed));
        let (mut r_epoch, r) = read_index(ring.rsvd.load(Ordering::Relaxed));

        let v = r + (count as u64);

        // since t <= r, r and t being in different epochs means means r is an
        // epoch ahead of t and thus must be less than the modulated value of r.
        if t_epoch != r_epoch {
            // We're already an epoch ahead so just ensure we do not pass t
            // again.
            if v > t {
                //println!("reserve {} > {}", v, t);
                return Err(());
            }
        } else {
            // We're in the same epoch so we can move forward up to t in the
            // next epoch.
            if v > (t + (R as u64)) {
                println!("reserve {} > {}", v, t + (R as u64));
                return Err(());
            }
        }

        let mut vr = v % (R as u64);

        // track epoch for r
        if v >= (R as u64) {
            r_epoch = !r_epoch;
        }
        if r_epoch {
            vr = toggle_epoch(vr);
        }

        ring.rsvd.store(vr, Ordering::Relaxed);

        // allocate some frames
        let buf = unsafe { &mut *ring.buf.get() };
        let buf_iter = buf.alloc(count as u64);

        // assign frame addresses to ring elements
        let elements = unsafe { &mut *ring.elements.get() };
        for (i, (addr, _)) in buf_iter.enumerate() {
            let idx = ((r as usize) + i) % R;
            //println!("R[{}] = {}", idx, addr);
            elements[idx].addr = addr;
        }

        Ok(RingIterator::<R, N, F> {
            ring: UnsafeCell::new(ring.clone()),
            index: r,
            count: count as u64,
        })
    }

    pub fn produce(&self, count: usize) -> Result<(), ()> {
        let ring = unsafe { &mut *self.ring.get() };

        // Enforce ring invariant h <= r
        let (r_epoch, mut r) = read_index(ring.rsvd.load(Ordering::Relaxed));
        let (mut h_epoch, h) = read_index(ring.head.load(Ordering::Relaxed));

        // The index r always leads h, so if they have different epochs, r is in
        // the epoch ahead of h.
        if r_epoch != h_epoch {
            r = r + R as u64;
        }

        let v = h + (count as u64);

        if v > r {
            //println!("produce {}@{} > {}@{}", v, h_epoch, r, r_epoch);
            return Err(());
        }

        let mut vr = v % (R as u64);

        //track epoch for h
        if v >= (R as u64) {
            h_epoch = !h_epoch
        }
        if h_epoch {
            vr = toggle_epoch(vr);
        }

        ring.head.store(vr, Ordering::Relaxed);

        Ok(())
    }

    pub fn write(&self, e: RingElement, buf: &[u8]) {
        //println!("writing {} ({})", e.addr, buf.len());

        let r = unsafe { &mut *self.ring.get() };
        let b = unsafe { &mut *r.buf.get() };
        let f = unsafe { &mut *b.frames.get() };
        let frame_data = unsafe { &mut *f[e.addr].data.get() };
        frame_data[..buf.len()].copy_from_slice(buf);
        f[e.addr].len = buf.len();
    }
}

impl<'a, const R: usize, const N: usize, const F: usize> fmt::Debug for RingProducer<R, N, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingProducer")
            .field("ring", unsafe { &*self.ring.get() })
            .finish()
    }
}

unsafe impl<const R: usize, const N: usize, const F: usize> Sync for RingProducer<R, N, F> {}

unsafe impl<const R: usize, const N: usize, const F: usize> Send for RingProducer<R, N, F> {}

/// A [`RingConsumer`] provides read access to a [`Ring`].
pub struct RingConsumer<const R: usize, const N: usize, const F: usize> {
    ring: UnsafeCell<Arc<Ring<R, N, F>>>,
}

fn read_index(index: u64) -> (bool, u64) {
    let epoch = ((1 << 63) & index) != 0;
    let value = (!(1 << 63)) & index;
    (epoch, value)
}

fn toggle_epoch(index: u64) -> u64 {
    index ^ (1 << 63)
}

impl<'a, const R: usize, const N: usize, const F: usize> RingConsumer<R, N, F> {
    pub fn consume(&self, count: usize) -> Result<(), ()> {
        let r = unsafe { &mut *self.ring.get() };

        // Enforce ring invariant t <= h
        let (h_epoch, mut h) = read_index(r.head.load(Ordering::Relaxed));
        let (mut t_epoch, t) = read_index(r.tail.load(Ordering::Relaxed));

        if h_epoch != t_epoch {
            h = h + R as u64;
        }

        let v = t + (count as u64);

        if v > h {
            return Err(());
        }

        //TODO force R to be a power of 2 so we can use & to implement the modulo?
        let mut vr = v % (R as u64);

        // track epoch for t
        if v >= (R as u64) {
            t_epoch = !t_epoch
        }
        if t_epoch {
            vr = toggle_epoch(vr);
        }

        r.tail.store(vr, Ordering::Relaxed);

        Ok(())
    }

    pub fn consumable(&self) -> RingIterator<R, N, F> {
        let r = unsafe { &mut *self.ring.get() };

        // calculate the distance from h to t
        let (h_epoch, mut h) = read_index(r.head.load(Ordering::Relaxed));
        let (t_epoch, t) = read_index(r.tail.load(Ordering::Relaxed));

        if h_epoch != t_epoch {
            h = h + R as u64;
        }

        RingIterator::<R, N, F> {
            ring: UnsafeCell::new(r.clone()),
            index: t,
            count: (h - t) as u64,
        }
    }

    pub fn read(&self, e: RingElement) -> &[u8] {
        let r = unsafe { &mut *self.ring.get() };
        let b = unsafe { &mut *r.buf.get() };
        let f = unsafe { &mut *b.frames.get() };
        unsafe {
            let fr = &f[e.addr];
            //println!("reading {} ({})", e.addr, fr.len);
            let data = &*fr.data.get();
            &data[..fr.len]
        }
    }
}

impl<'a, const R: usize, const N: usize, const F: usize> fmt::Debug for RingConsumer<R, N, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RingConsumer")
            .field("ring", unsafe { &*self.ring.get() })
            .finish()
    }
}

unsafe impl<const R: usize, const N: usize, const F: usize> Sync for RingConsumer<R, N, F> {}

unsafe impl<const R: usize, const N: usize, const F: usize> Send for RingConsumer<R, N, F> {}

#[cfg(test)]
mod tests {
    use std::str::from_utf8;
    use std::sync::Arc;
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use super::*;

    use rand::Rng;

    #[test]
    fn toggle() {
        let x = 47u64;
        assert_eq!((false, 47u64), read_index(x));

        let x = toggle_epoch(x);
        assert_eq!((true, 47u64), read_index(x));

        let x = toggle_epoch(x);
        assert_eq!((false, 47u64), read_index(x));
    }

    #[test]
    fn fb_alloc() {
        let fba = Arc::new(FrameBuffer::<1024, 1518>::new());

        let fb = fba.clone();
        let t1 = spawn(move || {
            let x = fb.alloc(4);
            println!("t1: {}", x.count());
        });

        let fb = fba.clone();
        let t2 = spawn(move || {
            let x = fb.alloc(7);
            println!("t2: {}", x.count());
        });

        t1.join().unwrap();
        t2.join().unwrap();

        println!("{}", fba.index.load(Ordering::Relaxed));

        assert_eq!(fba.index.load(Ordering::Relaxed), 11);
    }

    #[test]
    fn ring_a_ding_ding() {
        let banter = [
            b"do you know the muffin man?".as_slice(),
            b"the muffin man?",
            b"the muffin man!",
            b"why yes i know the muffin man ...",
            b"... the muffin man is me!",
        ];

        let fb = Arc::new(FrameBuffer::<1024, 1518>::new());
        let (producer, consumer) = ring::<64, 1024, 1518>(fb);

        let t1 = spawn(move || {
            println!("Producer: @1 {:#?}", producer);

            let frame_pointers = producer.reserve(5).unwrap();
            for (i, fp) in frame_pointers.enumerate() {
                producer.write(fp, banter[i]);
            }

            println!("Producer: @2 {:#?}", producer);

            producer.produce(1).unwrap();
            sleep(Duration::from_secs(1));
            producer.produce(1).unwrap();
            sleep(Duration::from_secs(1));
            producer.produce(1).unwrap();
            sleep(Duration::from_secs(1));
            producer.produce(1).unwrap();
            sleep(Duration::from_secs(1));
            producer.produce(1).unwrap();
            sleep(Duration::from_secs(1));

            println!("Producer: @3 {:#?}", producer);
        });

        let t2 = spawn(move || {
            for _ in 0..10 {
                sleep(Duration::from_secs(1));

                let frame_pointers = consumer.consumable();
                println!(">>>>>>>");
                for fp in frame_pointers {
                    let content = consumer.read(fp);
                    println!("{}", from_utf8(content).unwrap());
                }
                println!("<<<<<<<\n");
                println!("Consumer: {:#?}", consumer);
                println!("=======\n");
            }

            for (i, fp) in consumer.consumable().enumerate() {
                let content = consumer.read(fp);
                assert_eq!(banter[i], content);
            }

            println!("Consumer: @1 {:#?}", consumer);

            consumer.consume(5).unwrap();

            println!("Consumer: @2 {:#?}", consumer);

            assert_eq!(consumer.consumable().count(), 0);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn ring_rollover() {
        let fb = Arc::new(FrameBuffer::<8, 64>::new());
        let (p, c) = ring::<8, 8, 64>(fb);

        let t1 = spawn(move || {
            let mut i = 0;

            let fps = p.reserve(8).unwrap();
            for fp in fps {
                p.write(fp, format!("frame{}", i).as_bytes());
                i += 1;
            }
            p.produce(8).unwrap();

            sleep(Duration::from_secs(1));

            let fps = p.reserve(8).unwrap();
            for fp in fps {
                p.write(fp, format!("frame{}", i).as_bytes());
                i += 1;
            }
            p.produce(8).unwrap();
        });

        let t2 = spawn(move || {
            let mut i = 0;

            sleep(Duration::from_millis(250));

            println!("Consumer: @0 {:#?}", c);

            for fp in c.consumable() {
                let content = from_utf8(c.read(fp)).unwrap();
                assert_eq!(content, &format!("frame{}", i));
                i += 1;
            }
            c.consume(8).unwrap();

            sleep(Duration::from_secs(2));

            println!("Consumer: @1 {:#?}", c);

            for fp in c.consumable() {
                let content = from_utf8(c.read(fp)).unwrap();
                assert_eq!(content, &format!("frame{}", i));
                i += 1;
            }

            println!("Consumer: @2 {:#?}", c);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn base_xfer() {
        // Transfer 1,000,000 1500 byte frames (1.5 GB)
        const R: usize = 1024;
        const N: usize = 4096;
        const F: usize = 1500;
        const K: u32 = 1000000;

        let fb = Arc::new(FrameBuffer::<N, F>::new());
        let (p, c) = ring::<R, N, F>(fb);

        let t1 = spawn(move || {
            // create test data we're going to send the full 1500 bytes per
            // packet, but only bother writing a 4 byte integer in each packet.
            let mut data = Vec::new();
            for i in 0..K {
                data.push(i.to_be_bytes());
            }

            let mut rng = rand::thread_rng();
            let mut i = 0;
            loop {
                let count = usize::min(rng.gen_range(0..10), (K - i) as usize);
                let fps = match p.reserve(count) {
                    Ok(fps) => fps,
                    Err(_) => continue,
                };
                for fp in fps {
                    p.write(fp, data[i as usize].as_slice());
                    i += 1;
                }
                p.produce(count).unwrap();
                if i >= K {
                    break;
                }
            }

            println!("producer finished");
        });

        let t2 = spawn(move || {
            let mut total = 0u32;
            loop {
                let mut count = 0;
                let consumable = c.consumable();
                //println!("{:?}", consumable);
                for fp in consumable {
                    let content = c.read(fp);
                    let x = u32::from_be_bytes(content.try_into().unwrap());
                    //println!("{} ==> {}", total, x);
                    assert_eq!(x, total);

                    //println!("{} ==> {}", total, content.len());

                    total += 1;
                    count += 1;
                }
                if count == 0 {
                    continue;
                }
                //println!("consuming {}", count);
                //println!("{:?}", c);
                c.consume(count).unwrap();
                if total >= K {
                    break;
                }
            }

            println!("consumer finished");
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
