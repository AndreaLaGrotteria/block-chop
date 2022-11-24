use crate::broker::LoadBroker;
use std::mem;
use talk::sync::promise::Solver;
use tokio::sync::mpsc::UnboundedReceiver;

type UsizeOutlet = UnboundedReceiver<usize>;

enum State {
    Locked(Solver<()>),
    Unlocked,
    Freed,
}

impl LoadBroker {
    pub(in crate::broker::load_broker) async fn lockstep(
        _flow_id: usize,
        lock_solvers: Vec<Solver<()>>,
        mut free_outlet: UsizeOutlet,
        lockstep_delta: usize,
    ) {
        // Initialize `State`s (initially, all `Locked`)

        let mut states = lock_solvers
            .into_iter()
            .map(|lock_solver| State::Locked(lock_solver))
            .collect::<Vec<_>>();

        // `unlock()` the first `lockstep_delta` `states`

        for index in 0..lockstep_delta {
            if let Some(state) = states.get_mut(index) {
                state.unlock();
            }
        }

        // Progressively unlock `states`: when the first `n` `states` are
        // first `Freed`, `unlock()` the `n + lockstep_delta`-th `State`

        let mut head = 0;

        loop {
            let free = if let Some(free) = free_outlet.recv().await {
                free
            } else {
                // `LoadBroker` has dropped, return
                return;
            };

            *states.get_mut(free).unwrap() = State::Freed;

            while let Some(State::Freed) = states.get(head) {
                if let Some(state) = states.get_mut(head + lockstep_delta) {
                    state.unlock();
                }

                head += 1;
            }
        }
    }
}

impl State {
    fn unlock(&mut self) {
        let state = mem::replace(self, State::Unlocked);

        match state {
            State::Locked(solver) => {
                solver.solve(());
            }
            _ => unreachable!(),
        }
    }
}
