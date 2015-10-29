#!/usr/bin/env ruby
require 'optparse'
require 'csv'

QUERIES = {
  "tpch" => {
    :roles => {
      "10g" => true,
      "100g" => true
    },
    :queries => {
      "1" => "TPCHQuery1",
      "3" => "TPCHQuery3",
      "5" => "TPCHQuery5",
      "6" => "TPCHQuery6",
      "18" => "TPCHQuery18",
      "22" => "TPCHQuery22"
    }
  },
  "amplab" => {
    :roles => {
      "sf5" => true
    },
    :queries => {
      "1" => "AmplabQuery1",
      "2" => "AmplabQuery2",
      "3" => "AmplanQuery3"
    }
  },
  "ml" => {
    :roles => {
      "10g" => true,
      "100g" => true
    },
    :queries => {
      "kmeans" => "KMeans",
      "sgd" => "SGD",
    }
  },
  "graph" => {
    :roles => {
      "twitter" => true,
    },
    :queries => {
      "pagerank" => "PageRank",
    }
  }
}

# Utils
def exp_id(experiment, query, role)
  return "#{experiment}-#{query}-#{role}"
end

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
    :worker_container => "sparkWorker",
    :deploy_dir  => "../deploy",
    :spark_home => "/software/spark-1.2.1/",
    :spark_master => "spark://qp-hm1:7077",
    :jar_file => "scala-2.10/spark-benchmarks_2.10-1.0.jar",
    :includes => [],
    :excludes => [],
    :trials => 1,
    :profile => false,
    :profile_output => "/tmp/perf.data",
    :profile_freq  => 10,
    :jfr => true,
    :jfr_output => "/tmp/record.jfr",
    :jfr_opts => "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=/tmp/record.jfr",
    :machines => (1..8).map{|x| "qp-hm" + x.to_s},
    :result_dir => "results/#{Time.now.strftime("%m-%d-%Y-%H-%M-%S")}"
  }
  $stats = {}

  usage = "Usage: #{$PROGRAM_NAME} options"
  if ARGF.argv.empty?
    puts usage
  end

  parser = OptionParser.new do |opts|
    opts.banner = usage
    opts.on("-1", "--build", "Build Spark Benchmark jar file")  { $options[:build] = true }
    opts.on("-2", "--run", "Run Spark Benchmarks")  { $options[:run] = true }

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

  summarize()
end

def cleanup_client()
  system "docker kill spark_client"
  system "docker rm spark_client"
end

def client_cmd(dockeropts, cmd)
  cmd = "docker run --name spark_client --net=host -v /tmp:/build #{dockeropts} damsl/spark #{cmd}"
  puts cmd
  return cmd
end

def run_profile(cmd, hosts)
  profile_cmd = "docker exec -d #{$options[:worker_container]} #{cmd}"
  ansible_cmd = "ansible #{hosts} -i #{$options[:deploy_dir]}/hosts.ini -m shell -a \"#{profile_cmd}\""
  puts ansible_cmd
  system ansible_cmd
end

# Build a Spark Jar in a docker container using sbt. Requires simple.sbt and src/ directory. JAR is stashed in /build which is /tmp on host.
def build()
  cmd = client_cmd("-v /tmp:/build -v #{Dir.pwd}:/src -t", "/src/sbin/package_jar.sh")
  system cmd
  cleanup_client()
end

# Run a Spark Jar in a docker container using spark-submit.
def run()
  for experiment, description in QUERIES do
    for query, class_name in description[:queries] do
      for role,_ in description[:roles] do
        if !select?(experiment, query, role)
          next
        end
        1.upto($options[:trials]) do |trial|

          # Initiate profiling through ansible prior to an experiment.
          if $options[:profile]
            profile_desc = "#{experiment}-#{query}-#{role}"
            profile_cmd  = "/sbin/spark_perf_start.sh #{$options[:profile_freq]} #{$options[:profile_output]}-#{profile_desc} 1000000"
            run_profile("workers", profile_cmd)
          end

          # Setup Java Flight Recorder
          extra_java_opts = ""
          if $options[:jfr]
            extra_java_opts += "spark.executor.extraJavaOptions=#{$options[:jfr_opts]}"
          end

          run_cmd = "#{$options[:spark_home]}/bin/spark-submit --master #{$options[:spark_master]} --conf \"#{extra_java_opts}\" --class #{class_name} /build/#{$options[:jar_file]} #{role}"
          full_cmd = client_cmd("", run_cmd)

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
         
          # Collect Java Flight Recorder results
          extra_java_opts = ""
          if $options[:jfr]
            collect_jfr(exp_id(experiment, role, query), trial)
          end

          cleanup_client()

          # Stop profiling.
          if $options[:profile]
            profile_cmd = "/sbin/spark_perf_stop.sh"
            run_profile("workers", profile_cmd)
          end

        end
      end
    end
  end
end

def collect_jfr(id, trial)
  dir = "#{$options[:result_dir]}/#{id}/#{trial}"
  `mkdir -p #{dir}`
  puts "Collecting all Java Flight Recorder Data"
  for host in $options[:machines]
    puts "Collecting: #{host}..."
    `scp #{host}:/tmp/record.jfr #{dir}/#{host}.jfr`
  end
end

def summarize()
  puts "Summary"
  `mkdir -p #{$options[:result_dir]}`
  summary_path = "#{$options[:result_dir]}/times.csv"
  CSV.open(summary_path, 'w') { |file|
    for key, val in $stats
      for trial in val
        csv_row = [key[:experiment], key[:role], key[:query], trial]
        file << csv_row
      end
      sum = val.reduce(:+)
      cnt = val.size
      avg = 1.0 * sum / cnt
      var = val.map{|x| (x - avg) * (x - avg)}.reduce(:+) / (1.0 * cnt)
      dev = Math.sqrt(var)
      id = exp_id(key[:experiment], key[:query], key[:role])
      puts "\t#{id} => Succesful Trials: #{cnt}/#{$options[:trials]}. Avg: #{avg}. StdDev: #{dev}"
    end
  }
end

if __FILE__ == $0
  main
end
