#!/usr/bin/env ruby
require 'optparse'

# TODO gather statistics over multiple trials

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
    :spark_home => "/software/spark-1.2.0/",
    :spark_master => "spark://qp-hm1:7077",
    :jar_file => "scala-2.10/spark-benchmarks_2.10-1.0.jar",
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
end

# Build a Spark Jar in a docker container using sbt. Requires simple.sbt and src/ directory. JAR is stashed in /tmp/scala...
def build()
  cmd = "docker run -v #{Dir.pwd}:/src -v /tmp:/build -t damsl/spark /src/sbin/package_jar.sh"
  puts cmd
  system cmd
end

# Run a Spark Jar in a docker container using spark-submit.
def run()
  for experiment, description in QUERIES do
    for query, class_name in description[:queries] do
      for role,_ in description[:roles] do
        if !select?(experiment, query, role)
          next
        end
        1.upto($options[:trials]) do |_|
          run_cmd = "#{$options[:spark_home]}/bin/spark-submit --master #{$options[:spark_master]} --class #{class_name} /build/#{$options[:jar_file]} #{role}"
          full_cmd = "docker run -v /tmp:/build --net=host damsl/spark #{run_cmd}"
          puts full_cmd
          # Run full_cmd, with output stored and printed in real time
          output = []
          result = ""
          r, io = IO.pipe
          fork do
            system(full_cmd, out: io, err: :out)
          end
          io.close
          for l in r.each_line do
            puts l
            output << l.chomp
            if l.chomp.start_with?('Elapsed:')
              result = l
            end
          end
          puts result
        end
      end
    end
  end
end

if __FILE__ == $0
  main
end
