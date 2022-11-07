mod broker_submission;
mod modes;
mod observable;
mod server_submission;
mod utils;

use broker_submission::BrokerSubmission;
use observable::Observable;

fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `heartbeat` data statistics tool

        Choose one of the following:
          --shallow-broker (string) path to `Broker` / `LoadBroker` `heartbeat` data
          --shallow-server (string) path to a `Server` `heartbeat` data

        Options:
          --drop-front (default 0.) number of seconds to drop from the beginning of the `heartbeat` data
        ",
    );

    let shallow_broker = args.get_string_result("shallow-broker").ok();
    let shallow_server = args.get_string_result("shallow-server").ok();

    let drop_front = args.get_float("drop-front");

    if [&shallow_broker, &shallow_server]
        .into_iter()
        .flatten()
        .count()
        != 1
    {
        println!("Please select one of the available statistical modes.");
        return;
    }

    if let Some(path) = shallow_broker {
        modes::shallow_broker(path, drop_front);
    } else if let Some(path) = shallow_server {
        modes::shallow_server(path, drop_front);
    }
}
