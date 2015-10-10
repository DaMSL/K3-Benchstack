#!/usr/bin/env ruby
require 'optparse'

QUERIES = {
  "tpch" => {
    :roles => {
      "10g" => true,
      "100g" => true
    },
    :queries => {
      "1"  => "tpch.TPCHQuery1",
      "3"  => "tpch.TPCHQuery3",
      "5"  => "tpch.TPCHQuery5",
      "6"  => "tpch.TPCHQuery6",
      "18" => "tpch.TPCHQuery18",
      "22" => "tpch.TPCHQuery22"
    }
  },
  "amplab" => {
    :roles => {
      "sf5" => true
    },
    :queries => {
      "1" => "amplab.AmplabQuery1",
      "2" => "amplab.AmplabQuery2",
      "3" => "amplab.AmplanQuery3"
    }
  },
  "ml" => {
    :roles => {
      "10g" => true,
      "100g" => true
    },
    :queries => {
      "kmeans" => "ml.KMeans",
      "sgd"    => "ml.SGD",
    }
  },
  "graph" => {
    :roles => {
      "twitter" => true,
    },
    :queries => {
      "pagerank" => "graph.PageRank",
    }
  }
}

# Utils
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
    :worker_container => "flink_slave"
    :deploy_dir       => "../deploy",
    :flink_home       => "/software/flink-0.9.1/",
    :flink_master     => "qp-hm1:6123",
    :jar_file         => "/flink/flink-tpch-1.0-SNAPSHOT.jar",
    :includes         => [],
    :excludes         => [],
    :trials           => 1,
    :profile          => false,
    :profile_output   => "/flink/perf.data",
    :profile_freq     => 10,
  }
  $stats = {}

  usage = "Usage: #{$PROGRAM_NAME} options"
  if ARGF.argv.empty?
    puts usage
  end

  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-1", "--build", "Build Flink Benchmark jar file")  { $options[:build] = true }
    opts.on("-2", "--run", "Run Flink Benchmarks")  { $options[:run] = true }

    opts.on("-t", "--trials num", Integer, "Number of trials per query") { |i| $options[:trials] = i }

    opts.on("-i", "--include pat1,pat2,pat3", Array, "Patterns to Include") { |is| $options[:includes] = is }
    opts.on("-e", "--exclude pat1,pat2,pat3", Array, "Patterns to Exclude") { |es| $options[:excludes] = es }
  end
  parser.parse!

  if $options[:build]
    build()
  end

  if $options[:run]
    run()
  end

  summary()
end

# Build flink jar, while mounting a /src directory, and a /flink output directory
def build()
  cmd = "docker run -v #{Dir.pwd}:/src -v /data/flink:/flink damsl/flink /src/sbin/package_jar.sh"
  puts cmd
  system cmd
end

# Run a Flink Jar in a docker container using the flink command line tool.
def run()
  for experiment, description in QUERIES do
    for query, class_name in description[:queries] do
      for role,_ in description[:roles] do
        if !select?(experiment, query, role)
          next
        end
        1.upto($options[:trials]) do |_|

          # Initiate profiling through ansible prior to an experiment.
          if $options[:profile]
            profile_desc = "#{experiment}-#{query}-#{role}"
            profile_cmd  = "/sbin/flink_perf_start.sh #{$options[:profile_freq]} #{$options[:profile_output]}-#{profile_desc} 1000000"
            perf_cmd     = "docker exec -d #{$options[:worker_container]} #{profile_cmd}"
            ansible_cmd  = "ansible-playbook -i #{$options[:deploy_dir]}/hosts.ini #{$options[:deploy_dir]}/plays/perf_start.yml"
            system(ansible_cmd)
          end

          class_prefix = "edu.jhu.cs.damsl.k3"

          run_cmd = "#{$options[:flink_home]}/bin/flink run --jobmanager #{$options[:flink_master]} --class #{class_prefix}.#{class_name} #{$options[:jar_file]} #{role}"
          full_cmd = "docker run -v /tmp:/build --net=host damsl/flink #{run_cmd}"
          puts full_cmd
          # Run full_cmd, with output stored and printed in real time
          result = ""
          r, io = IO.pipe
          fork do
            system(full_cmd, out: io, err: :out)
          end
          io.close
          for l in r.each_line do
            puts l
            if l.chomp.start_with?('Elapsed:')
              result = l.chomp.split(" ")[1].to_i
            end
          end
          # Store time associated with this trial
          key = {:role => role, :experiment => experiment, :query => query}
          if not $stats.has_key? key
            $stats[key] = [result]
          else
            $stats[key] << result
          end

          # Stop profiling.
          if $options[:profile]
            profile_cmd = "/sbin/flink_perf_stop.sh"
            perf_cmd    = "docker exec -d #{$options[:worker_container]} #{profile_cmd}"
            ansible_cmd = "ansible-playbook -i #{$options[:deploy_dir]}/hosts.ini #{$options[:deploy_dir]}/plays/perf_stop.yml"
            system(ansible_cmd)
          end

        end
      end
    end
  end
end

def summary()
  puts "Summary"
  for key, val in $stats
    sum = val.reduce(:+)
    cnt = val.size
    avg = 1.0 * sum / cnt
    var = val.map{|x| (x - avg) * (x - avg)}.reduce(:+) / (1.0 * cnt)
    dev = Math.sqrt(var)
    puts "\t#{key} => Succesful Trials: #{cnt}/#{$options[:trials]}. Avg: #{avg}. StdDev: #{dev}"
  end
end

if __FILE__ == $0
  main
end
