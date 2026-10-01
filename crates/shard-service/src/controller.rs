use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub shard: ShardId,
    pub epoch: u64,
    pub owner: GroupId,
    pub transfer: Option<TransferId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
pub enum AbortStage {
    Started,
    Fenced { image: ImageProof },
    Installed { installation: InstalledProof },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum Phase {
    Started,
    Aborted {
        abort: AbortProof,
        before: AbortStage,
        source_recovered: Option<Origin>,
        destination_recovered: Option<Origin>,
    },
    Fenced {
        image: ImageProof,
    },
    Installed {
        installation: InstalledProof,
    },
    Owned {
        ownership: OwnershipProof,
    },
    Activated {
        activation: ActivationProof,
    },
    Cleaned {
        activation: ActivationProof,
        cleaned: Origin,
    },
    Complete {
        activation: ActivationProof,
        cleaned: Origin,
        completed: Origin,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub request_id: String,
    pub movement: Move,
    pub phase: Phase,
}
impl Transfer {
    pub fn complete(&self) -> bool {
        matches!(
            self.phase,
            Phase::Complete { .. }
                | Phase::Aborted {
                    source_recovered: Some(_),
                    destination_recovered: Some(_),
                    ..
                }
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControllerCommand {
    AcquireMaintenance {
        request_id: String,
        target: GroupId,
        operation: AdminOperation,
        expected_generation: Option<Origin>,
    },
    CompleteMaintenance {
        request_id: String,
        ticket_origin: Origin,
        core_request_id: String,
        record: AdminRecord,
        observed_applied: Origin,
    },
    Initialize {
        groups: Vec<GroupId>,
        owners: Vec<GroupId>,
    },
    Begin {
        request_id: String,
        shard: ShardId,
        expected_epoch: u64,
        destination: GroupId,
    },
    Fenced {
        image: ImageProof,
    },
    Installed {
        installation: InstalledProof,
    },
    Own {
        id: TransferId,
    },
    Abort {
        id: TransferId,
    },
    EndpointAborted {
        abort: AbortProof,
        recovered: Origin,
    },
    Activated {
        activation: ActivationProof,
    },
    Cleaned {
        id: TransferId,
        cleaned: Origin,
    },
    Complete {
        id: TransferId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ControllerReply {
    Initialized { origin: Origin },
    Transfer(Box<Transfer>),
    Maintenance(Box<MaintenanceTicket>),
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Controller {
    pub applied: u64,
    pub initialized_at: Option<Origin>,
    pub groups: Vec<GroupId>,
    pub routes: Vec<Route>,
    pub transfers: BTreeMap<String, Transfer>,
    #[serde(default)]
    pub maintenance: BTreeMap<String, MaintenanceTicket>,
}

impl Controller {
    /// Rejections still consume their committed log index; effects are atomic.
    pub fn apply(
        &mut self,
        index: u64,
        command: ControllerCommand,
    ) -> Result<Result<ControllerReply, Error>, String> {
        if self.applied.checked_add(1) != Some(index) {
            return Err("controller application gap".into());
        }
        let mut next = self.clone();
        next.applied = index;
        let result = next.execute(index, command).and_then(|reply| {
            next.check_capacity()?;
            Ok(reply)
        });
        if result.is_ok() {
            *self = next;
        } else {
            self.applied = index;
        }
        Ok(result)
    }
    pub fn advance(&mut self, index: u64) -> Result<(), String> {
        if self.applied.checked_add(1) != Some(index) {
            return Err("controller application gap".into());
        }
        self.applied = index;
        Ok(())
    }
    fn execute(
        &mut self,
        index: u64,
        command: ControllerCommand,
    ) -> Result<ControllerReply, Error> {
        let origin = Origin {
            group: CONTROLLER,
            index,
        };
        if let ControllerCommand::Initialize { groups, owners } = command {
            if let Some(initialized) = self.initialized_at {
                if self.groups == groups
                    && self.routes.iter().map(|r| r.owner).collect::<Vec<_>>() == owners
                    && self.transfers.is_empty()
                    && self.maintenance.is_empty()
                {
                    return Ok(ControllerReply::Initialized {
                        origin: initialized,
                    });
                }
                return Err(Error::AlreadyInitialized);
            }
            if groups.is_empty()
                || groups.len() > MAX_GROUPS
                || owners.is_empty()
                || owners.len() > MAX_SHARDS
                || groups.contains(&CONTROLLER)
                || groups.windows(2).any(|pair| pair[0] >= pair[1])
                || owners.iter().any(|owner| !groups.contains(owner))
            {
                return Err(Error::InvalidInput);
            }
            self.groups = groups;
            self.routes = owners
                .into_iter()
                .enumerate()
                .map(|(shard, owner)| Route {
                    shard: shard as u16,
                    epoch: 1,
                    owner,
                    transfer: None,
                })
                .collect();
            self.initialized_at = Some(origin);
            return Ok(ControllerReply::Initialized { origin });
        }
        if self.initialized_at.is_none() {
            return Err(Error::NotInitialized);
        }
        if let ControllerCommand::AcquireMaintenance {
            request_id,
            target,
            operation,
            expected_generation,
        } = command
        {
            if !maintenance_request_id_valid(&request_id)
                || !maintenance_operation_valid(&operation)
            {
                return Err(Error::InvalidInput);
            }
            if target != CONTROLLER && !self.groups.contains(&target) {
                return Err(Error::WrongGroup);
            }
            if let Some(existing) = self.maintenance.get(&request_id) {
                return if existing.target == target && existing.operation == operation {
                    Ok(ControllerReply::Maintenance(Box::new(existing.clone())))
                } else {
                    Err(Error::PayloadMismatch)
                };
            }
            if expected_generation != self.maintenance_generation() {
                return Err(Error::StaleGeneration);
            }
            if self.active_maintenance().is_some() || self.transfers.values().any(|t| !t.complete())
            {
                return Err(Error::Busy);
            }
            if self.maintenance.len() >= MAX_MAINTENANCE_TICKETS {
                return Err(Error::Capacity);
            }
            let ticket = MaintenanceTicket {
                request_id: request_id.clone(),
                target,
                operation,
                origin,
                previous_generation: expected_generation,
                completion: None,
            };
            self.maintenance.insert(request_id, ticket.clone());
            return Ok(ControllerReply::Maintenance(Box::new(ticket)));
        }
        if let ControllerCommand::CompleteMaintenance {
            request_id,
            ticket_origin,
            core_request_id,
            record,
            observed_applied,
        } = command
        {
            let ticket = self
                .maintenance
                .get(&request_id)
                .ok_or(Error::WrongTransfer)?;
            if ticket.origin != ticket_origin || ticket.core_request_id() != core_request_id {
                return Err(Error::WrongTransfer);
            }
            if !ticket.record_matches(&record, observed_applied) {
                return Err(Error::WrongPhase);
            }
            if let Some(completion) = &ticket.completion {
                return if completion.record == record {
                    Ok(ControllerReply::Maintenance(Box::new(ticket.clone())))
                } else {
                    Err(Error::PayloadMismatch)
                };
            }
            if self.maintenance.values().any(|previous| {
                previous.target == ticket.target
                    && previous.origin.index < ticket.origin.index
                    && previous.completion.as_ref().is_some_and(|completion| {
                        record.first_index <= completion.observed_applied.index
                            || record.first_term < completion.record.final_term.unwrap_or(0)
                    })
            }) {
                return Err(Error::WrongPhase);
            }
            let completion = MaintenanceCompletion {
                record,
                observed_applied,
                released: origin,
            };
            if canonical_bytes(&completion, MAINTENANCE_COMPLETION_RESERVE).is_err()
                || (ticket.target == CONTROLLER && observed_applied.index >= origin.index)
            {
                return Err(Error::InvalidInput);
            }
            let ticket = self
                .maintenance
                .get_mut(&request_id)
                .expect("checked ticket");
            ticket.completion = Some(completion);
            return Ok(ControllerReply::Maintenance(Box::new(ticket.clone())));
        }
        if let ControllerCommand::Begin {
            request_id,
            shard,
            expected_epoch,
            destination,
        } = command
        {
            if request_id.is_empty() || request_id.len() > 128 {
                return Err(Error::InvalidInput);
            }
            if let Some(existing) = self.transfers.get(&request_id) {
                if existing.movement.id.shard == shard
                    && existing.movement.previous_epoch == expected_epoch
                    && existing.movement.destination == destination
                {
                    return Ok(ControllerReply::Transfer(Box::new(existing.clone())));
                }
                return Err(Error::PayloadMismatch);
            }
            if self.active_maintenance().is_some() {
                return Err(Error::Busy);
            }
            if self.transfers.len() >= MAX_TRANSFERS {
                return Err(Error::Capacity);
            }
            let route = self
                .routes
                .get(usize::from(shard))
                .ok_or(Error::InvalidInput)?;
            if route.epoch != expected_epoch {
                return Err(Error::StaleRoute);
            }
            if destination == route.owner || !self.groups.contains(&destination) {
                return Err(Error::WrongGroup);
            }
            if self.transfers.values().any(|t| {
                !t.complete()
                    && [t.movement.source, t.movement.destination]
                        .iter()
                        .any(|group| *group == route.owner || *group == destination)
            }) {
                return Err(Error::Busy);
            }
            let epoch = expected_epoch.checked_add(1).ok_or(Error::Capacity)?;
            let movement = Move {
                id: TransferId {
                    shard,
                    epoch,
                    controller_index: index,
                },
                source: route.owner,
                destination,
                previous_epoch: expected_epoch,
            };
            self.routes[usize::from(shard)].transfer = Some(movement.id.clone());
            let transfer = Transfer {
                request_id: request_id.clone(),
                movement,
                phase: Phase::Started,
            };
            self.transfers.insert(request_id, transfer.clone());
            return Ok(ControllerReply::Transfer(Box::new(transfer)));
        }
        let id = match &command {
            ControllerCommand::Fenced { image } => &image.movement.id,
            ControllerCommand::Installed { installation } => &installation.image.movement.id,
            ControllerCommand::Own { id }
            | ControllerCommand::Abort { id }
            | ControllerCommand::Cleaned { id, .. }
            | ControllerCommand::Complete { id } => id,
            ControllerCommand::Activated { activation } => {
                &activation.ownership.installation.image.movement.id
            }
            ControllerCommand::EndpointAborted { abort, .. } => &abort.movement.id,
            _ => unreachable!("handled initial commands"),
        }
        .clone();
        let transfer = self
            .transfers
            .values_mut()
            .find(|t| t.movement.id == id)
            .ok_or(Error::WrongTransfer)?;
        match command {
            ControllerCommand::Abort { .. } => {
                if !matches!(transfer.phase, Phase::Aborted { .. }) {
                    let before = match &transfer.phase {
                        Phase::Started => AbortStage::Started,
                        Phase::Fenced { image } => AbortStage::Fenced {
                            image: image.clone(),
                        },
                        Phase::Installed { installation } => AbortStage::Installed {
                            installation: installation.clone(),
                        },
                        _ => return Err(Error::WrongPhase),
                    };
                    transfer.phase = Phase::Aborted {
                        abort: AbortProof {
                            movement: transfer.movement.clone(),
                            controller: origin,
                        },
                        before,
                        source_recovered: None,
                        destination_recovered: None,
                    };
                }
            }
            ControllerCommand::EndpointAborted { abort, recovered } => {
                let Phase::Aborted {
                    abort: expected,
                    source_recovered,
                    destination_recovered,
                    before,
                } = &mut transfer.phase
                else {
                    return Err(Error::WrongPhase);
                };
                if !abort.valid() || abort != *expected || !recovered.valid() {
                    return Err(Error::WrongTransfer);
                }
                let lower = match (&*before, recovered.group) {
                    (AbortStage::Fenced { image }, group) if group == transfer.movement.source => {
                        image.fence.index
                    }
                    (AbortStage::Installed { installation }, group)
                        if group == transfer.movement.source =>
                    {
                        installation.image.fence.index
                    }
                    (AbortStage::Installed { installation }, group)
                        if group == transfer.movement.destination =>
                    {
                        installation.installed.index
                    }
                    _ => 0,
                };
                if recovered.index <= lower {
                    return Err(Error::WrongTransfer);
                }
                let receipt = if recovered.group == transfer.movement.source {
                    source_recovered
                } else if recovered.group == transfer.movement.destination {
                    destination_recovered
                } else {
                    return Err(Error::WrongGroup);
                };
                if let Some(previous) = receipt {
                    if *previous != recovered {
                        return Err(Error::PayloadMismatch);
                    }
                } else {
                    *receipt = Some(recovered);
                }
                if transfer.complete() {
                    let route = &mut self.routes[usize::from(id.shard)];
                    // A duplicate old completion cannot clear a later transfer.
                    if route.transfer.as_ref() == Some(&id) {
                        route.transfer = None;
                    }
                }
            }
            ControllerCommand::Fenced { image } => {
                if !image.valid() || image.movement != transfer.movement {
                    return Err(Error::WrongTransfer);
                }
                match &transfer.phase {
                    Phase::Started => transfer.phase = Phase::Fenced { image },
                    Phase::Fenced { image: old } if *old == image => {}
                    _ => return Err(Error::WrongPhase),
                }
            }
            ControllerCommand::Installed { installation } => {
                if !installation.valid() || installation.image.movement != transfer.movement {
                    return Err(Error::WrongTransfer);
                }
                match &transfer.phase {
                    Phase::Fenced { image } if *image == installation.image => {
                        transfer.phase = Phase::Installed { installation }
                    }
                    Phase::Installed { installation: old } if *old == installation => {}
                    _ => return Err(Error::WrongPhase),
                }
            }
            ControllerCommand::Own { .. } => match &transfer.phase {
                Phase::Installed { installation } => {
                    let route = &mut self.routes[usize::from(id.shard)];
                    if route.owner != transfer.movement.source
                        || route.epoch != transfer.movement.previous_epoch
                        || route.transfer.as_ref() != Some(&id)
                    {
                        return Err(Error::WrongTransfer);
                    }
                    route.owner = transfer.movement.destination;
                    route.epoch = id.epoch;
                    transfer.phase = Phase::Owned {
                        ownership: OwnershipProof {
                            installation: installation.clone(),
                            controller: origin,
                        },
                    };
                }
                Phase::Owned { .. } => {}
                _ => return Err(Error::WrongPhase),
            },
            ControllerCommand::Activated { activation } => {
                if !activation.valid() {
                    return Err(Error::InvalidInput);
                }
                match &transfer.phase {
                    Phase::Owned { ownership } if *ownership == activation.ownership => {
                        transfer.phase = Phase::Activated { activation }
                    }
                    Phase::Activated { activation: old } if *old == activation => {}
                    _ => return Err(Error::WrongPhase),
                }
            }
            ControllerCommand::Cleaned { cleaned, .. } => {
                if cleaned.group != transfer.movement.source || !cleaned.valid() {
                    return Err(Error::WrongGroup);
                }
                match &transfer.phase {
                    Phase::Activated { activation }
                        if cleaned.index > activation.ownership.installation.image.fence.index =>
                    {
                        transfer.phase = Phase::Cleaned {
                            activation: activation.clone(),
                            cleaned,
                        }
                    }
                    Phase::Cleaned { cleaned: old, .. } if *old == cleaned => {}
                    _ => return Err(Error::WrongPhase),
                }
            }
            ControllerCommand::Complete { .. } => match &transfer.phase {
                Phase::Cleaned {
                    activation,
                    cleaned,
                } => {
                    self.routes[usize::from(id.shard)].transfer = None;
                    transfer.phase = Phase::Complete {
                        activation: activation.clone(),
                        cleaned: *cleaned,
                        completed: origin,
                    };
                }
                Phase::Complete { .. } => {}
                _ => return Err(Error::WrongPhase),
            },
            _ => unreachable!("handled initial commands"),
        }
        Ok(ControllerReply::Transfer(Box::new(transfer.clone())))
    }

    pub fn maintenance_generation(&self) -> Option<Origin> {
        self.maintenance
            .values()
            .map(|ticket| ticket.origin)
            .max_by_key(|origin| origin.index)
    }
    pub fn active_maintenance(&self) -> Option<&MaintenanceTicket> {
        self.maintenance
            .values()
            .find(|ticket| ticket.completion.is_none())
    }
    fn check_capacity(&self) -> Result<(), Error> {
        let reserved = 4096 + u64::MAX.to_string().len() - self.applied.to_string().len()
            + if self.active_maintenance().is_some() {
                MAINTENANCE_COMPLETION_RESERVE
            } else {
                0
            };
        canonical_bytes(self, MAX_ACTOR_BYTES.saturating_sub(reserved)).map(|_| ())
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.initialized_at.is_none() {
            if !self.groups.is_empty()
                || !self.routes.is_empty()
                || !self.transfers.is_empty()
                || !self.maintenance.is_empty()
            {
                return Err("uninitialized controller contains routes".into());
            }
            return Ok(());
        }
        let initialized = self.initialized_at.expect("checked");
        if initialized.group != CONTROLLER
            || initialized.index == 0
            || initialized.index > self.applied
            || self.groups.is_empty()
            || self.groups.len() > MAX_GROUPS
            || self.groups.contains(&CONTROLLER)
            || self.groups.windows(2).any(|p| p[0] >= p[1])
            || self.routes.is_empty()
            || self.routes.len() > MAX_SHARDS
            || self.transfers.len() > MAX_TRANSFERS
            || self.maintenance.len() > MAX_MAINTENANCE_TICKETS
        {
            return Err("invalid controller configuration".into());
        }
        let mut live = BTreeSet::new();
        let mut identities = BTreeSet::new();
        let mut begin_indices = BTreeSet::new();
        for (number, route) in self.routes.iter().enumerate() {
            if usize::from(route.shard) != number
                || route.epoch == 0
                || !self.groups.contains(&route.owner)
            {
                return Err("invalid route".into());
            }
        }
        for (request, transfer) in &self.transfers {
            let movement = &transfer.movement;
            if request != &transfer.request_id
                || request.is_empty()
                || request.len() > 128
                || !movement.valid()
                || movement.id.controller_index > self.applied
                || movement.id.controller_index <= initialized.index
                || !identities.insert(movement.id.clone())
                || !begin_indices.insert(movement.id.controller_index)
                || !self.groups.contains(&movement.source)
                || !self.groups.contains(&movement.destination)
            {
                return Err("invalid transfer identity".into());
            }
            let route = self
                .routes
                .get(usize::from(movement.id.shard))
                .ok_or("transfer shard absent")?;
            if !transfer.complete() {
                if route.transfer.as_ref() != Some(&movement.id)
                    || !live.insert(movement.source)
                    || !live.insert(movement.destination)
                {
                    return Err("overlapping active transfer".into());
                }
                let owned = matches!(
                    transfer.phase,
                    Phase::Owned { .. } | Phase::Activated { .. } | Phase::Cleaned { .. }
                );
                if (route.owner, route.epoch)
                    != (
                        if owned {
                            movement.destination
                        } else {
                            movement.source
                        },
                        if owned {
                            movement.id.epoch
                        } else {
                            movement.previous_epoch
                        },
                    )
                {
                    return Err("route disagrees with transfer phase".into());
                }
            }
            let image = match &transfer.phase {
                Phase::Started => None,
                Phase::Aborted {
                    abort,
                    before,
                    source_recovered,
                    destination_recovered,
                } => {
                    if !abort.valid()
                        || abort.movement != *movement
                        || abort.controller.index > self.applied
                    {
                        return Err("invalid controller abort proof".into());
                    }
                    for (receipt, group) in [
                        (source_recovered, movement.source),
                        (destination_recovered, movement.destination),
                    ] {
                        if receipt.is_some_and(|receipt| !receipt.valid() || receipt.group != group)
                        {
                            return Err("invalid endpoint abort receipt".into());
                        }
                    }
                    let image_before = match before {
                        AbortStage::Started => None,
                        AbortStage::Fenced { image } => Some(image),
                        AbortStage::Installed { installation } => Some(&installation.image),
                    };
                    if image_before.is_some_and(|image| {
                        source_recovered.is_some_and(|receipt| receipt.index <= image.fence.index)
                    }) || matches!(before,AbortStage::Installed {installation} if destination_recovered.is_some_and(|receipt|receipt.index<=installation.installed.index))
                    {
                        return Err("abort receipt precedes endpoint preparation".into());
                    }
                    match before {
                        AbortStage::Started => None,
                        AbortStage::Fenced { image } => Some(image),
                        AbortStage::Installed { installation } => {
                            if !installation.valid() {
                                return Err("invalid pre-abort installation".into());
                            }
                            Some(&installation.image)
                        }
                    }
                }
                Phase::Fenced { image } => Some(image),
                Phase::Installed { installation } => {
                    if !installation.valid() {
                        return Err("invalid installation proof".into());
                    }
                    Some(&installation.image)
                }
                Phase::Owned { ownership } => {
                    if !ownership.valid() || ownership.controller.index > self.applied {
                        return Err("invalid ownership proof".into());
                    }
                    Some(&ownership.installation.image)
                }
                Phase::Activated { activation }
                | Phase::Cleaned { activation, .. }
                | Phase::Complete { activation, .. } => {
                    if !activation.valid() || activation.ownership.controller.index > self.applied {
                        return Err("invalid activation proof".into());
                    }
                    Some(&activation.ownership.installation.image)
                }
            };
            if image.is_some_and(|image| !image.valid() || image.movement != *movement) {
                return Err("transfer proof identity mismatch".into());
            }
            if let Phase::Cleaned {
                activation,
                cleaned,
            }
            | Phase::Complete {
                activation,
                cleaned,
                ..
            } = &transfer.phase
            {
                if cleaned.group != movement.source
                    || cleaned.index <= activation.ownership.installation.image.fence.index
                {
                    return Err("invalid cleanup proof".into());
                }
            }
            if let Phase::Complete {
                completed,
                ref activation,
                ..
            } = transfer.phase
            {
                if completed.group != CONTROLLER
                    || completed.index > self.applied
                    || completed.index <= activation.ownership.controller.index
                {
                    return Err("invalid completion proof".into());
                }
            }
        }
        let mut tickets: Vec<_> = self.maintenance.values().collect();
        tickets.sort_by_key(|ticket| ticket.origin.index);
        let mut previous = None;
        let mut previous_release = initialized.index;
        let mut target_progress = BTreeMap::new();
        for (position, ticket) in tickets.iter().enumerate() {
            if self.maintenance.get(&ticket.request_id) != Some(*ticket)
                || !ticket.valid()
                || ticket.origin.index <= previous_release
                || ticket.origin.index > self.applied
                || ticket.previous_generation != previous
                || !begin_indices.insert(ticket.origin.index)
                || (ticket.target != CONTROLLER && !self.groups.contains(&ticket.target))
            {
                return Err("invalid retained maintenance ticket or generation chain".into());
            }
            match &ticket.completion {
                Some(completion) => {
                    if completion.released.index > self.applied {
                        return Err(
                            "maintenance completion exceeds applied controller state".into()
                        );
                    }
                    if target_progress
                        .get(&ticket.target)
                        .is_some_and(|&(applied, term)| {
                            completion.record.first_index <= applied
                                || completion.record.first_term < term
                        })
                    {
                        return Err("maintenance target history regresses".into());
                    }
                    target_progress.insert(
                        ticket.target,
                        (
                            completion.observed_applied.index,
                            completion.record.final_term.expect("validated completion"),
                        ),
                    );
                    previous_release = completion.released.index;
                }
                None => {
                    if position + 1 != tickets.len()
                        || self.transfers.values().any(|t| !t.complete())
                    {
                        return Err("overlapping active maintenance or handoff".into());
                    }
                    previous_release = self.applied;
                }
            }
            previous = Some(ticket.origin);
        }
        for route in &self.routes {
            // Retained history is also the route's epoch/owner chain. Validating
            // each proof alone would allow a corrupted terminal route to invent
            // an owner or skip epochs while every individual proof still fits.
            let mut history: Vec<_> = self
                .transfers
                .values()
                .filter(|t| t.movement.id.shard == route.shard)
                .collect();
            history.sort_by_key(|t| t.movement.id.controller_index);
            let mut owner = history.first().map_or(route.owner, |t| t.movement.source);
            let mut epoch = 1;
            let mut previous_controller_index = initialized.index;
            for (position, transfer) in history.iter().enumerate() {
                let movement = &transfer.movement;
                if movement.source != owner
                    || movement.previous_epoch != epoch
                    || movement.id.controller_index <= previous_controller_index
                    || (!transfer.complete() && position + 1 != history.len())
                {
                    return Err("inconsistent retained route history".into());
                }
                let ownership = match &transfer.phase {
                    Phase::Owned { ownership } => Some(ownership),
                    Phase::Activated { activation }
                    | Phase::Cleaned { activation, .. }
                    | Phase::Complete { activation, .. } => Some(&activation.ownership),
                    _ => None,
                };
                if ownership.is_some() {
                    owner = movement.destination;
                    epoch = movement.id.epoch;
                }
                previous_controller_index = match &transfer.phase {
                    Phase::Complete { completed, .. } => completed.index,
                    Phase::Aborted { abort, .. } => abort.controller.index,
                    _ => ownership.map_or(movement.id.controller_index, |p| p.controller.index),
                };
            }
            if (route.owner, route.epoch) != (owner, epoch) {
                return Err("route differs from retained ownership history".into());
            }
            if route.transfer.as_ref().is_some_and(|id| {
                !self
                    .transfers
                    .values()
                    .any(|t| !t.complete() && &t.movement.id == id)
            }) {
                return Err("route references missing transfer".into());
            }
        }
        self.check_capacity()
            .map_err(|_| "controller exceeds bound".into())
    }
}
