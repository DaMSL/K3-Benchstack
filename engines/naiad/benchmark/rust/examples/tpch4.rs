/// TPCH4, based on the TPCH17 example.

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
use differential_dataflow::operators::join::JoinBy;
use differential_dataflow::operators::consolidate::ConsolidateExt;

fn main() {

    let items_pattern  = std::env::args().nth(1).unwrap();
    let orders_pattern = std::env::args().nth(2).unwrap();
    let delimiter      = std::env::args().nth(3).unwrap();

    timely::execute_from_args(std::env::args().skip(4), move |computation| {

        let comm_index = computation.index();
        let comm_peers = computation.peers();

        let mut start = time::precise_time_s();

        let epoch = Rc::new(RefCell::new(0u64));
        let clone = epoch.clone();

        let (mut items, mut orders) = computation.scoped(|builder| {
            let (item_input, items) = builder.new_input::<((u64, u32, u32), i32)>();
            let (orders_input, orders) = builder.new_input::<((u64, u32, String), i32)>();

            let items = items.filter(|x| (x.0).1 < (x.0).2).map(|x| ((x.0).0, x.1));
            let orders = orders.filter(|x| 19930701 <= (x.0).1 && (x.0).1 < 19931001).map(|x| (((x.0).0, (x.0).2), x.1) );

            let items = orders.join_by_u(&items, |o| (o.0, o.1), |l| (l, ()), |_, o, _| (o.clone(), 1))
                              .group(|key,vals,out| { 
                                  let mut count = 0;
                                  for (val, mult) in vals {
                                    count+= mult
                                  }
                                  out.push((count, 1)) 
                              })
                              .consolidate()
                              .inspect(|x| println!("Q4 result: {:?} {:?}", x.0, x.1));

            (item_input, orders_input)
        });

        // read the lineitems input file
        let mut items_buffer = Vec::new();
        if let Ok(items_file) = File::open(format!("{}{}", items_pattern, comm_index)) {
            let items_reader =  BufReader::new(items_file);
            for (index, line) in items_reader.lines().enumerate() {
                if index % comm_peers == comm_index {
                    let text = line.ok().expect("read error");
                    let mut fields = text.split(&delimiter);
                    let orderkey    = fields.next().unwrap().parse::<u64>().unwrap();
                    fields.next(); //1
                    fields.next(); 
                    fields.next();
                    fields.next();
                    fields.next();
                    fields.next();
                    fields.next();
                    fields.next();
                    fields.next();
                    fields.next(); // 10
                    let commitdate  = fields.next().unwrap().parse::<String>().unwrap().replace("-","").parse::<u32>().unwrap();
                    let receiptdate = fields.next().unwrap().parse::<String>().unwrap().replace("-","").parse::<u32>().unwrap();
                    items_buffer.push(((orderkey, commitdate, receiptdate), 1i32));
                }
            }
        }
        else { println!("worker {}: did not find input {}{}", comm_index, items_pattern, comm_index); }

        // read the orders input file
        let mut orders_buffer = Vec::new();
        if let Ok(orders_file) = File::open(format!("{}{}", orders_pattern, comm_index)) {
            let orders_reader = BufReader::new(orders_file);
            for (index, line) in orders_reader.lines().enumerate() {
                if index % comm_peers == comm_index {
                    let text = line.ok().expect("read error");
                    let mut fields = text.split(&delimiter);
                    let orderkey      = fields.next().unwrap().parse::<u64>().unwrap();
                    fields.next();
                    fields.next();
                    fields.next();
                    let orderdate     =  fields.next().unwrap().parse::<String>().unwrap().replace("-","").parse::<u32>().unwrap();
                    let orderpriority = fields.next().unwrap().parse::<String>().unwrap();
                    orders_buffer.push((((orderkey, orderdate, orderpriority)), 1i32));
                }
            }
        }
        else { println!("worker {}: did not find input {}{}", computation.index(), orders_pattern, comm_index); }


        println!("Q4 data loaded at {}", time::precise_time_s() - start);
        start = time::precise_time_s();

        for order in orders_buffer.drain_temp() { orders.send(order); }
        orders.close();

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

        println!("Q4 computation finished at {}", time::precise_time_s() - start);
        println!("Q4 rate: {}", (item_count as f64) / (time::precise_time_s() - start));
    });
}
