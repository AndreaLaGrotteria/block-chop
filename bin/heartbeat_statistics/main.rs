mod observable;
mod shallow_broker;

use observable::Observable;

fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `heartbeat` data statistics tool

        Choose one of the following:
          --shallow-broker (string) path to `Broker` / `LoadBroker` `heartbeat` data

        Options:
          --drop-front (default 0.) number of seconds to drop from the beginning of the `heartbeat` data
        ",
    );

    let shallow_broker = args.get_string_result("shallow-broker").ok();

    let drop_front = args.get_float("drop-front");

    if [&shallow_broker].into_iter().flatten().count() != 1 {
        println!("Please select one of the available statistical modes.");
        return;
    }

    if let Some(path) = shallow_broker {
        shallow_broker::shallow_broker(path, drop_front);
    }
}
