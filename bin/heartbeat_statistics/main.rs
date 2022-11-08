mod broker_submission;
mod modes;
mod observable;
mod server_submission;
mod utils;

use broker_submission::BrokerSubmission;
use observable::Observable;
use server_submission::ServerSubmission;

fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `heartbeat` data statistics tool

        Choose one of the following:
          --shallow-broker (string) path to `Broker` / `LoadBroker` `heartbeat` data
          --shallow-server (string) path to a `Server` `heartbeat` data

        Options:
          --start (float) beginning of the `heartbeat` data window of interest
          --duration (float) breadth of the `heartbeat` data window of interest
        ",
    );

    let shallow_broker = args.get_string_result("shallow-broker").ok();
    let shallow_server = args.get_string_result("shallow-server").ok();

    let start = args.get_float_result("start").unwrap_or(0.);
    let duration = args.get_float_result("duration").unwrap_or(1e6);

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
        modes::shallow_broker(path, start, duration);
    } else if let Some(path) = shallow_server {
        modes::shallow_server(path, start, duration);
    }
}
