#!/usr/bin/env ruby
require 'optparse'

# TODO gather statistics over multiple trials 

QUERIES = {
  "tpch" => {
    :roles => {
      "10g" => "sql/tpch/create_load_tpch.sql",
      "100g" => "sql/tpch/create_load_tpch.sql"
    },
    :queries => {
      "1" => "sql/tpch/1.sql",
      "3" => "sql/tpch/3.sql",
      "5" => "sql/tpch/5.sql",
      "6" => "sql/tpch/6.sql",
      "18" => "sql/tpch/18.sql",
      "22" => "sql/tpch/22.sql"
    }
  },
  "amplab" => {
    :roles => {
      "sf5" => "sql/amplab/create_load_amplab.sql"
    },
    :queries => {
      "1" => "sql/amplab/1.sql",
      "2" => "sql/amplab/2.sql",
      "3" => "sql/amplab/3.sql"
    }
  }
}

def select?(experiment, query, role = nil)
  excluded = $options[:excludes].any? do |pattern|
    check_filter(pattern, experiment, query, role)
  end

  included = $options[:includes].any? do |pattern|
    check_filter(pattern, experiment, query, role)
  end

  return !excluded || included
end

def check_filter(pattern, experiment, query, role = nil)
  (expected_experiment, expected_query, expected_role) = pattern.split("/")
  return ((Regexp.new expected_experiment) =~ experiment) &&
         ((Regexp.new expected_query) =~  query) &&
         (role.nil? || (Regexp.new expected_role) =~ role)
end

def main()
  STDOUT.sync = true
  $options = { 
    :impala_host => "qp-hm1.damsl.cs.jhu.edu",
    :includes => [],
    :excludes => [],
    :trials => 1
  }

  usage = "Usage: #{$PROGRAM_NAME} options"
  if ARGF.argv.empty?
    puts usage
  end
  
  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-1", "--load", "Create and Load Tables for Impala")  { $options[:load] = true }
    opts.on("-2", "--run", "Run Impala Benchmarks")  { $options[:run] = true }
    
    opts.on("-t", "--trials num", Integer, "Number of trials per query") { |i| $options[:trials] = i }

    opts.on("-i", "--include pat1,pat2,pat3", Array, "Patterns to Include") { |is| $options[:includes] = is }
    opts.on("-e", "--exclude pat1,pat2,pat3", Array, "Patterns to Exclude") { |es| $options[:excludes] = es }
  end
  parser.parse!

  if $options[:load]
    load_tables()
  end

  if $options[:run]
    run()
  end
end 

# Create and load Impala tables. Requrires the sbin/ sql/ directories.
def load_tables()
  for experiment, description in QUERIES do
    for role,schema_file in description[:roles] do
      cmd = "docker run -v #{Dir.pwd}:/build damsl/impala /build/sbin/create_tables.sh #{$options[:impala_host]} #{schema_file} #{role}"
      puts cmd
    end
  end
end

# Run Impala queries in a docker container using impala-shell
def run()
  for experiment, description in QUERIES do
    for query, query_file in description[:queries] do
      for role,_ in description[:roles] do
        if !select?(experiment, query, role)
          next
        end
        1.upto($options[:trials]) do |_|
          cmd = "docker run -v #{Dir.pwd}:/build damsl/impala /build/sbin/run_query.sh #{$options[:impala_host]} #{query_file} #{role}"
          puts cmd 
          #output = []
          #result = ""
          #r, io = IO.pipe
          #fork do
          #  system(cmd, out: io, err: :out)
          #end
          #io.close
          #for l in r.each_line do
          #  puts l
          #  output << l.chomp
          #  if l.chomp.start_with?('Fetched')
          #    result = l 
          #  end
          #end 
          #puts result
        end
      end
    end
  end
end

if __FILE__ == $0
  main
end
