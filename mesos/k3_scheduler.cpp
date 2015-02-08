/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <libgen.h>
#include <time.h>
#include <iostream>
#include <string>
#include <fstream>

#include <yaml-cpp/yaml.h>

#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>

#include <yaml-cpp/yaml.h>

#include <glog/logging.h>

#include <stout/exit.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>

#define TOTAL_PEERS 4
#define MASTER "zk://192.168.0.10:2181,192.168.0.11:2181,192.168.0.18:2181/mesos"
#define FILE_SERVER "http://192.168.0.10:8002"
#define DOCKER_IMAGE "damsl/k3-mesos2"
#define CONDENSED true
#define MEM_REQUESTED 80000

using namespace mesos;

using boost::lexical_cast;

using std::cout;
using std::cerr;
using std::endl;
using std::flush;
using std::string;
using std::vector;
using std::map;

//const int32_t CPUS_PER_TASK = 1;
const double CPUS_PER_TASK = 1.0;
const int32_t MEM_PER_TASK = 512;

string taskState [7]= {"STARTING", "RUNNING", "FINISHED", "FAILED", "KILLED", "LOST","STAGING"};

static map<string, string> ip_addr = {{"damsl","192.168.0.1"},
      {"qp1","192.168.0.10"},
      {"qp2","192.168.0.11"},
      {"qp3","192.168.0.15"},
      {"qp4","192.168.0.16"},
      {"qp5","192.168.0.17"},
      {"qp6","192.168.0.18"},
      {"qp-hd1","192.168.0.24"},
      {"qp-hd2","192.168.0.25"},
      {"qp-hd3","192.168.0.26"},
      {"qp-hd4","192.168.0.27"},
      {"qp-hd5","192.168.0.28"},
      {"qp-hd6","192.168.0.29"},
      {"qp-hd7","192.168.0.30"},
      {"qp-hd8","192.168.0.31"},
      {"qp-hd9","192.168.0.32"},
      {"qp-hd10","192.168.0.33"},
      {"qp-hd11","192.168.0.34"},
      {"qp-hd12","192.168.0.35"},
      {"qp-hd13","192.168.0.36"},
      {"qp-hd14","192.168.0.37"},
      {"qp-hd15","192.168.0.38"},
      {"qp-hd16","192.168.0.39"},
      {"qp-hm1","192.168.0.40"},
      {"qp-hm2","192.168.0.41"},
      {"qp-hm3","192.168.0.42"},
      {"qp-hm4","192.168.0.43"},
      {"qp-hm5","192.168.0.44"},
      {"qp-hm6","192.168.0.45"},
      {"qp-hm7","192.168.0.46"},
      {"qp-hm8","192.168.0.47"}  };

std::string currTime() {
  time_t rawtime;
  struct tm * timeinfo;
  char buffer [80];

  time (&rawtime);
  timeinfo = localtime (&rawtime);

  strftime (buffer,80,"%I:%M:%S%p",timeinfo);
  return std::string(buffer);

}


// Forward Declare
//ExecutorInfo makeExecutor (string customExecutor, map<string, string> mounts);
ExecutorInfo makeExecutor (string programBinary, YAML::Node hostParams,
      map<string, string> mounts);

// TODO:  Better defined data structure
class peerProfile {
public:
  peerProfile (string _ip, string _port) :
    ip(_ip), port(_port)  {}
  
  YAML::Node getAddr () {
    YAML::Node me;
    me.push_back(ip);
    me.push_back(port);
    return me;
  }

  string ip;
  string port;
};

class hostProfile {
public:
  int numPeers () {
    return peers.size();
  }
  
  void addPeer (int peer) {
    peers.push_back(peer);
  }
  
  double cpu;
  double mem;
  int  offer;
  vector<int> peers;
  vector<YAML::Node> params;     // Param list for ea peer
};

class KDScheduler : public Scheduler {
  public:
    KDScheduler(string _k3binary, int _totalPeers, YAML::Node _vars, bool log, int maxP, int pph, string rv, int start_port)
    {
      this->k3binary = _k3binary;
      this->totalPeers = _totalPeers;
      this->k3vars = _vars;
      this->logging = log;
      this->maxPartitions = maxP;
      this->peersPerHost = pph;
      this->resultVar = rv;
      this->startPort = start_port;
    }

  virtual ~KDScheduler() {}

  virtual void registered(SchedulerDriver*, const FrameworkID&, const MasterInfo&)  {
    LOG(INFO) << "[REGISTERED] K3 Framework REGISTERED with Mesos" << endl;
  }

  virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo) {
    LOG(INFO) << "[RE-REGISTERED]" << endl;
  }

  virtual void disconnected(SchedulerDriver* driver) {
    LOG(INFO) << "[DISCONNECTED]" << endl;
  }

  double getCpus(const Offer& offer) {
    double cpus;

    for (int i = 0; i < offer.resources_size(); i++) {
      const Resource& resource = offer.resources(i);
      if (resource.name() == "cpus" &&
        resource.type() == Value::SCALAR) {
        cpus = resource.scalar().value();
      } 
    }

    return cpus;
  }
  
  double getMem(const Offer& offer) {
    double mem;

    for (int i = 0; i < offer.resources_size(); i++) {
      const Resource& resource = offer.resources(i);
      if (resource.name() == "mem" &&
        resource.type() == Value::SCALAR) {
        mem = resource.scalar().value();
      } 
    }

    return mem;
  }

  // TODO replace template with actual type
  string getTextAttr(const Offer& offer, const string& key) {

    for (int i = 0; i < offer.attributes_size(); i++) {
      const Attribute& attrib = offer.attributes(i);
      if (attrib.name().compare (key) == 0) {
        return attrib.text().value();
      }
    }

    throw std::runtime_error("Attribute " + key + "not found in offer");

  }

  virtual void resourceOffers(SchedulerDriver* driver, const vector<Offer>& offers)  {
    cout << "[RESOURCE OFFER] " << offers.size() << " Offer(s)" << endl;

    int acceptedOffers = 0;
    // Iterate through each resource offer (1 offer per slave with available resources)
    //    Offer -> Vector of resources (cpu, mem, disk, etc)
    
    // 1. Determine set of offers to accept
    
    int unallocatedPeers = totalPeers - peersAssigned;
    size_t cur_offer = 0;

    while (cur_offer < offers.size() && unallocatedPeers > 0) {
      const Offer& offer = offers[cur_offer];
      bool accept_offer = true;

      double cpus = getCpus(offer);
      double mem = getMem(offer);

      if (k3vars["server_group"])  {
        string required_server = k3vars["server_group"].as<string>();
	string cat = getTextAttr(offer, "cat");

	if (cat != required_server) {
          cout << "Rejecting offer from " << offer.hostname() << endl;
          accept_offer = false;
        }
      } 
      
      if (k3vars["server"])  {
        string required_server = k3vars["server"].as<string>();
	if (offer.hostname() != required_server) {
          cout << "Rejecting offer from " << offer.hostname() << endl;
          accept_offer = false;
        }
      }
      
      if (accept_offer) {
        cout << " Accepted offer on " << offer.hostname() << endl;
        acceptedOffers++;
        hostProfile profile;
        int localPeers = unallocatedPeers;
        if (localPeers > cpus) {
          localPeers = cpus;
        }
        if (peersPerHost != 0 && localPeers > peersPerHost) {
          localPeers = peersPerHost;
        }
      
        // Create profile for Resource allocation & other info
        profile.cpu = localPeers;    //assume: 1 cpu per peer
        profile.mem = floor(mem);    // use all mem
        profile.offer = cur_offer;
        
        for (int p=0; p<localPeers; p++)  {
          int peerId = peersAssigned++;

          // TODO: PORT management
          string port = stringify(startPort + peerId);
          profile.addPeer(peerId);

          peerProfile peer (ip_addr[offer.hostname()], port);
          peerList.push_back(peer);
          unallocatedPeers--;
        }
        hostList[offer.hostname()] = profile;
        executorsAssigned++;
      }
      cur_offer++;
    }
    
    if (acceptedOffers == 0 || unallocatedPeers > 0)  {
      LOG(INFO) << "ERROR: Accepted no offers or some peers remain unallocated" << endl;
      return;
    }
    
    

        // Option B for doing resources management (see task-only scheduler for option A)
//        Option<Resources> resources = remaining.find(TASK_RESOURCES);
        // Option<Resources> resources = remaining.find(TASK_RESOURCES, role);
//        CHECK_SOME(resources);
//        task.mutable_resources()->MergeFrom(resources.get());
//        remaining -= resources.get();

  }

  virtual void offerRescinded(SchedulerDriver* driver, const OfferID& offerId) {}
  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)  {
    int taskId = lexical_cast<int>(status.task_id().value());

//    cout << "Task " << taskId << " -> " << taskState[status.state()] << endl;

    if (status.state() == TASK_FINISHED || status.state() == TASK_KILLED)  {
      executorsFinished++;
      cout << "Task " << taskId << " -> " << taskState[status.state()] << endl;
    }
    
                else if (status.state() == TASK_RUNNING)  {
      cout << "Task " << taskId << " -> " << taskState[status.state()] << endl;
    }

    else if (status.state() == TASK_LOST)  {
                        executorsFinished++;
      cout << "Task " << taskId << " -> " << taskState[status.state()] << endl;
                        cout << "Aborting!" << endl;
                        driver->stop();
    }
                else {
                  cout << "RECEIVED UNKNOWN STATUS! ABORTING! " << status.message() << endl;
                  driver->stop();
                }
               

    if (executorsFinished == executorsAssigned)
      driver->stop();
  }

  virtual void frameworkMessage(SchedulerDriver* driver, 
            const ExecutorID& executorId, 
            const SlaveID& slaveId, const string& data) {
    cout << "[FRMWK]: " << data << endl;
  }

  virtual void slaveLost(SchedulerDriver* driver, 
            const SlaveID& slaveId) 


        {
    cout << "slave lost" << endl;

        }
  virtual void executorLost(SchedulerDriver* driver,
            const ExecutorID& executorId, 
            const SlaveID& slaveId, int status) {


          cout << "Executor lost. Aborting" << endl;
          driver->stop();
        }
  virtual void error(SchedulerDriver* driver, const string& message) {
    cout << "error!" << endl;

  }

private:
//  const ExecutorInfo executor;
  int peersAssigned = 0;
  int peersFinished = 0;
  int executorsLaunched = 0;
  int executorsFinished = 0;
  int executorsAssigned = 0;
  int totalPeers;
  int maxPartitions = 0;
  int peersPerHost = 0;
  string resultVar;
  string k3binary;
  YAML::Node k3vars;
  string runpath;
  string fileServer;
  map<string, hostProfile> hostList;
  vector<peerProfile> peerList;
  bool logging;
  int startPort = 40000;
};


int main(int argc, char** argv)
{

  string master = MASTER;

  string k3binary = "countPeers";            
  string k3args_json;
  string k3args_yaml;
  string result_var="";
  YAML::Node k3vars;
        int peers_per_host = 0;
        int max_partitions = 0;
        int max_files = 0;
  int total_peers = (argc == 2) ? atoi(argv[1]) : 4;
  int start_port = 40000;
  

  //  Parse Command Line options
  namespace po = boost::program_options;
  vector <string> optionalVars;

  po::options_description desc("K3 Run options");
  desc.add_options()
        ("program", po::value<string>(&k3binary)->required(), "K3 executable program filename") 
    ("numpeers", po::value<int>(&total_peers)->required(), "# of K3 Peers to launch")
    ("help,h", "Print help message")
    ("logging,l", "Turn logging on")
    ("json,j", po::value<string>(&k3args_json), "K3 Program Arguments in JSON format")
    ("yaml,y", po::value<string>(&k3args_yaml), "YAML encoded input file")
                ("peers_per_host",po::value<int>(&peers_per_host), "Max Number of K3 Peers to launch on a single machine")
                ("max_partitions", po::value<int>(&max_partitions), "Hard limit on the number of partitions to use on each dataset")
    ("result_var", po::value<string>(&result_var),  "Log the result of a query as json ")
    ("start_port", po::value<int>(&start_port),  "Pick a starting port for k3 peers. Default: 40000 ");
                
  
  po::positional_options_description positionalOptions; 
    positionalOptions.add("program", 1); 
    positionalOptions.add("numpeers", 1);

  po::variables_map vm;
        bool logging = false;

  try {
    po::store(po::command_line_parser(argc, argv).options(desc) 
                  .positional(positionalOptions).run(), vm);
    if (vm.count("help") || vm.empty()) {
      cout << "K3 Distributed program framework backed by Mesos cluser" << endl << endl;
      cout << "Usage: " << endl;
      cout << "       k3Scheduler <k3-Binary> <#-Peers> [options]" << endl << endl;
      cout << desc << endl;
      return 0;
    }
    if (vm.count("json") && vm.count("file")) {
      cout << "Input Error. Can only have one of <yaml> or <json> input source." << endl << endl;
      cout << "Usage: " << endl;
      cout << "       k3Scheduler <k3-Binary> <#-Peers> [options]" << endl << endl;
      cout << desc << endl;
      return 0;
    }
    po::notify(vm);
    if (vm.count("json")) {
      k3vars = YAML::Load (k3args_json);
    }
    if (vm.count("yaml")) {
      k3vars = YAML::LoadFile (k3args_yaml);
    }
                if (vm.count("logging")) {
      logging = true;
    }
  }
  catch (boost::program_options::required_option& e)  {
    cerr << " ERROR: " << e.what() << endl << endl;
    cout << desc << endl;
    return 1;
  }
  catch (boost::program_options::error& e)  {
    cerr << " ERROR: " << e.what() << endl << endl;
    cout << desc << endl;
    return 1;
  }
  


//  KDScheduler scheduler(executor, k3binary, k3role, total_peers, k3vars);
//  KDScheduler scheduler(executor, k3binary, total_peers, k3vars);
  KDScheduler scheduler(k3binary, total_peers, k3vars, logging, max_partitions, peers_per_host, result_var, start_port);

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name(k3binary + "-" + stringify(total_peers) + "-" + currTime());
  framework.mutable_id()->set_value(k3binary + "-" + currTime());

  
  // FROM: Example Frame, left unchanged for adding Creds/ChckPts, etc..
  if (os::hasenv("MESOS_CHECKPOINT")) {
    cout << "Enabling checkpoint for the framework" << endl;
    framework.set_checkpoint(true);
  }

  MesosSchedulerDriver* driver;
  if (os::hasenv("MESOS_AUTHENTICATE")) {
    cout << "Enabling authentication for the framework" << endl;

    if (!os::hasenv("DEFAULT_PRINCIPAL")) {
      EXIT(1) << "Expecting authentication principal in the environment";
    }

    if (!os::hasenv("DEFAULT_SECRET")) {
      EXIT(1) << "Expecting authentication secret in the environment";
    }

    Credential credential;
    credential.set_principal(getenv("DEFAULT_PRINCIPAL"));
    credential.set_secret(getenv("DEFAULT_SECRET"));

    framework.set_principal(getenv("DEFAULT_PRINCIPAL"));

    driver = new MesosSchedulerDriver(
        &scheduler, framework, master, credential);
  } else {
    framework.set_principal("k3-framework");
    driver = new MesosSchedulerDriver(&scheduler, framework, master);
  }


      
        
  int status = driver->run() == DRIVER_STOPPED ? 0 : 1;

  // Ensure that the driver process terminates.
  driver->stop();

  delete driver;
  return status;
}

ExecutorInfo makeExecutor (string programBinary, YAML::Node hostParams,
      map<string, string> mounts) {
    string fileServer = FILE_SERVER;
  
  // CREATE The DOCKER Executor
    ExecutorInfo executor;
    executor.mutable_executor_id()->set_value("k3-executor");


  // Sets executor name to program name to pass along to slaves for execution
    executor.set_name(programBinary);
    executor.set_data(YAML::Dump(hostParams));

  //  executor.set_source("K3");    // not necessary to set this, 
                    // but may be useful for interacting frameworks
    
    /* Pull remote files from a "fileServer" location
      Files will be locally stored in $MESOS_SANDBOX location 
      and Mesos will automatically map host's sandbox dir to 
      a sandbox inside the docker container. Location accessible
      via the MESOS_SANDBOX env            */
    CommandInfo command;
    CommandInfo_URI * uri_e = command.add_uris();
    uri_e->set_value(fileServer + "/k3executor"); 
    uri_e->set_executable(true);
    uri_e->set_extract(false);
    
    CommandInfo_URI * k3_bin = command.add_uris();
    k3_bin->set_value(fileServer + "/" + programBinary);
    k3_bin->set_executable(true);
    k3_bin->set_extract(false);

    command.set_value("$MESOS_SANDBOX/k3executor");
    executor.mutable_command()->MergeFrom(command);

    // Alternate way of Adding Args, but shell=false which interferes stdout redirect
//    executor.mutable_command()->set_shell(false);
    //  executor.mutable_command()->add_arguments("3");

    /*  Build the Docker Container & map volume(s) */
    ContainerInfo::DockerInfo docker;
    
    /* Docker Image can be pulled, but requires libmesos lib installed which 
      is not baked into the sandbox when lauching a container
        NOTE: We could link it locally via setenv & mounted volume; however,
        that's not recommended. Mesos devs argue: if you're building 
        a custom executor, then you should provide a custom 
        docker image for the executor   */
    docker.set_image(DOCKER_IMAGE);

    // Build "container" & merge the DockerInfo object
    ContainerInfo container;
    container.set_type(container.DOCKER);
    container.mutable_docker()->MergeFrom(docker);

    for (auto &mnt : mounts) {
      cout << "MOUNTING: " << mnt.first << " to " << mnt.second << endl;
      Volume * volume = container.add_volumes();
      volume->set_host_path(mnt.first);
      volume->set_container_path(mnt.second);
      volume->set_mode(Volume_Mode_RO);
    }
    // Mount local volume inside Container -- useful for extracting output files
    Volume * volume = container.add_volumes();
    volume->set_host_path("/local/mesos");
    volume->set_container_path("/mnt/out");
    volume->set_mode(Volume_Mode_RW);
 

    executor.mutable_container()->MergeFrom(container);
    
    return executor;
}
