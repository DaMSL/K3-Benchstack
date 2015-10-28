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

use differential_dataflow::operators::group::Group;
use differential_dataflow::operators::consolidate::ConsolidateExt;

fn main() {

    let items_pattern = std::env::args().nth(1).unwrap();
    let delimiter = std::env::args().nth(2).unwrap();

    timely::execute_from_args(std::env::args().skip(3), move |computation| {

        let comm_index = computation.index();
        let comm_peers = computation.peers();

        let mut start = time::precise_time_s();
        let epoch = Rc::new(RefCell::new(0u64));

        let mut items = computation.scoped(|builder| {
            let (item_input, items) = builder.new_input::<((i32, i32, i32, u32), i32)>();
            items.filter(|x| {
                    //let date_between = 19940101 <= (x.0).3 && (x.0).3 < 19950101;
                    //let disc_between = 0.05 <= ((x.0).2 as f64) && ((x.0).2 as f64) <= 0.07;
                    let qty_lt = (x.0).0 < 24;
                    qty_lt
                    //date_between && disc_between && qty_lt
                 })
                 .map(|x| (((), (x.0).1 * (x.0).2), x.1))
                 .group(|key, vals, output| {
                   let mut revenue = 0;
                   for (eprice_times_disc, mult) in vals {
                      revenue += mult * eprice_times_disc;
                   }
                   output.push((revenue, 1));
                 })
                 .inspect(|x| println!("Q6 result: {:?}", x));
            item_input
        });

        // read the lineitems input file
        let mut items_buffer = Vec::new();
        if let Ok(items_file) = File::open(format!("{}{}", items_pattern, comm_index)) {
            let items_reader =  BufReader::new(items_file);
            for (index, line) in items_reader.lines().enumerate() {
                if index % comm_peers == comm_index {
                    let text = line.ok().expect("read error");
                    let mut fields = text.split(&delimiter);
                    fields.next();
                    fields.next();
                    fields.next();
                    fields.next();
                    let quantity       = fields.next().unwrap().parse::<f64>().unwrap() as i32;
                    let extended_price = fields.next().unwrap().parse::<f64>().unwrap() as i32;
                    let discount       = fields.next().unwrap().parse::<f64>().unwrap() as i32;
                    fields.next();
                    fields.next();
                    fields.next();
                    let shipdate       = fields.next().unwrap().parse::<String>().unwrap().replace("-","").parse::<u32>().unwrap();
                    items_buffer.push(((quantity, extended_price, discount, shipdate), 1i32));
                }
            }
        }
        else { println!("worker {}: did not find input {}{}", comm_index, items_pattern, comm_index); }

        println!("Q6 data loaded at {}", time::precise_time_s() - start);
        start = time::precise_time_s();

        let item_count = items_buffer.len();

        for (index, item) in items_buffer.drain_temp().enumerate() {
            items.send(item);
            items.advance_to(index as u64 + 1);
            //while *epoch.borrow() <= index as u64 {
            //    computation.step();
            //}
        }

        items.close();

        while computation.step() { }
        computation.step(); // shut down

        println!("Q6 computation finished at {}", time::precise_time_s() - start);
        println!("Q6 rate: {}", (item_count as f64) / (time::precise_time_s() - start));
    });
}
