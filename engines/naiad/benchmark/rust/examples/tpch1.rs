/// TPCH1, based on the TPCH17 example.

extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::rc::Rc;
use std::cell::RefCell;

use std::fs::File;
use std::io::{BufRead, BufReader};

use timely::*;
use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::drain::DrainExt;

use differential_dataflow::operators::group::GroupBy;

fn main() {

    let items_pattern = std::env::args().nth(1).unwrap();
    let delimiter = std::env::args().nth(2).unwrap();

    timely::execute_from_args(std::env::args().skip(3), move |computation| {

        let comm_index = computation.index();
        let comm_peers = computation.peers();

        let mut start = time::precise_time_s();
        let epoch = Rc::new(RefCell::new(0u64));

        let (mut items) = computation.scoped(|builder| {
            let (item_input, items) =
                builder.new_input::<(f64, f64, f64, f64, String, String, i32, i32)>();

            let items = items.filter(|x| x.6 < 19980902)
                             .map(|x| ((x.4, x.5), (x.0, x.1, x.2, x.3, x.7)))
                             .group(|key, vals, output| {
                               let (mut sqty, mut sbapr, mut sdipr, mut schg, mut sdi) = (0.0, 0.0, 0.0, 0.0, 0.0)
                               for (qty, eprice, disc, tx, cnt) in vals {
                                  sqty    += qty;
                                  sbapr   += eprice;
                                  sdipr   += eprice * (1 - disc);
                                  schg    += eprice * (1 - disc) * (1 + tx);
                                  scnt    += cnt;
                               }
                               output.push((sqty, sbapr, sdipr, schg, sqty / scnt, sbapr / scnt, sdi / scnt));
                             });

            item_input
        });

        // read the lineitems input file
        let mut items_buffer = Vec::new();
        if let Ok(items_file) = File::open(format!("{}-{}-{}", items_pattern, comm_index, comm_peers)) {
            let items_reader =  BufReader::new(items_file);
            for (index, line) in items_reader.lines().enumerate() {
                if index % comm_peers == comm_index {
                    let text = line.ok().expect("read error");
                    let mut fields = text.split(&delimiter);
                    fields.skip(4);
                    let quantity       = fields.next().unwrap().parse::<f64>().unwrap();
                    let extended_price = fields.next().unwrap().parse::<f64>().unwrap();
                    let discount       = fields.next().unwrap().parse::<f64>().unwrap();
                    let tax            = fields.next().unwrap().parse::<f64>().unwrap();
                    let returnflag     = fields.next().unwrap().parse::<String>().unwrap();
                    let linestatus     = fields.next().unwrap().parse::<String>().unwrap();
                    let shipdate       = fields.next().unwrap().parse::<i32>().unwrap();
                    items_buffer.push((quantity, extended_price, discount, tax, returnflag, linestatus, shipdate));
                }
            }
        }
        else { println!("worker {}: did not find input {}-{}-{}", comm_index, items_pattern, comm_index, comm_peers); }

        println!("Q1 data loaded at {}", time::precise_time_s() - start);
        start = time::precise_time_s();

        let item_count = items_buffer.len();

        for (index, item) in items_buffer.drain_temp().enumerate() {
            items.send(item);
            items.advance_to(index as u64 + 1);
            while *epoch.borrow() <= index as u64 {
                computation.step();
            }
        }

        items.close();

        while computation.step() { }
        computation.step(); // shut down

        println!("Q1 computation finished at {}", time::precise_time_s() - start);
        println!("Q1 rate: {}", (item_count as f64) / (time::precise_time_s() - start));
    });
}