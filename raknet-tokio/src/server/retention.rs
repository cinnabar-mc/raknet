//! Exact live routes and metadata-only reservations after network retirement.

use crate::session::wire::Datagram;
use crate::session::{SessionEvent, SessionLifetime};
use raknet::prelude::{RakServer, RakServerInput, RakSessionId, Sans};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc::Sender;

/// The original input sender and a weak observation of its exact typed handoff.
pub(super) struct Route {
    pub(super) input: Sender<SessionEvent>,
    pub(super) lifetime: SessionLifetime,
}

/// Admits committed wire output only for its exact live or retained original route.
pub(super) fn owns_wire(
    datagram: &Datagram,
    sessions: &HashMap<RakSessionId, Route>,
    retained: &HashMap<RakSessionId, SessionLifetime>,
) -> bool {
    sessions
        .get(&datagram.id())
        .map(|route| &route.lifetime)
        .or_else(|| retained.get(&datagram.id()))
        .is_some_and(|lifetime| datagram.belongs_to(lifetime))
}

/// Removes only the original current route; a late repeated ID has no further effect.
pub(super) fn retire(
    id: RakSessionId,
    sessions: &mut HashMap<RakSessionId, Route>,
    retained: &mut HashMap<RakSessionId, SessionLifetime>,
    server: &mut RakServer,
    now: Instant,
) {
    let Some(route) = sessions.remove(&id) else {
        return;
    };
    let _ = server.handle(RakServerInput::RemoveSession(id, now));
    if !route.lifetime.released() {
        retained.insert(id, route.lifetime);
    }
}

/// Retires completed actors while their original accepted handles remain separately owned.
pub(super) fn reap(
    sessions: &mut HashMap<RakSessionId, Route>,
    retained: &mut HashMap<RakSessionId, SessionLifetime>,
    server: &mut RakServer,
    maximum: usize,
    now: Instant,
) {
    let completed: Vec<_> = sessions
        .iter()
        .filter_map(|(id, route)| {
            (route.input.is_closed() || route.lifetime.actor_retired()).then_some(*id)
        })
        .collect();
    for id in completed {
        retire(id, sessions, retained, server, now);
    }
    retained.retain(|_, lifetime| !lifetime.released());
    // The core still owns live and pending reservations; subtract only retired ones.
    let _ = server.handle(RakServerInput::SetMaxConnections(
        maximum.saturating_sub(retained.len()),
    ));
}
