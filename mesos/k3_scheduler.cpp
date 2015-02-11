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
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <tuple>
#include <sstream>
#include <thread>

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

#include "webserver/server_http.hpp"

#define TOTAL_PEERS 4
#define MASTER "zk://192.168.0.10:2181,192.168.0.11:2181,192.168.0.18:2181/mesos"
#define FILE_SERVER "http://192.168.0.11:8002"
#define DOCKER_IMAGE "damsl/k3-mesos2"
#define CONDENSED true
#define MEM_REQUESTED 80000


using namespace mesos;
using namespace std;

using boost::lexical_cast;

using SimpleWeb::Server;
using SimpleWeb::HTTP;

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
      map<string, string> mounts, int id);

class PeerGroup {
  public:
    string server_;
    string server_group_;
    int peers_;
    int peers_per_host_;
    YAML::Node globals_;
    YAML::Node data_;
    YAML::Node yaml_;

    PeerGroup(YAML::Node config) {
      server_ = config["server"] ? parseField<string>("server", config) : "";
      server_group_ = config["server_group"] ? parseField<string>("server_group", config) : "";
      peers_ = parseField<int>("peers", config);
      peers_per_host_ = config["peers_per_host"] ? parseField<int>("peers_per_host", config) : 0;
      globals_ = config["k3_globals"] ? config["k3_globals"] : globals_;
      data_ = config["k3_data"] ? config["k3_data"] : data_;
      yaml_ = config;

      // TODO: constraint checking (e.g. no server and server_group)

    }

  private:
    std::runtime_error missingField(string key, YAML::Node config) {
      string yaml = YAML::Dump(config);
      return std::runtime_error("Failed to find key '" + key + "' in yaml: " + yaml);
    }

    template <class T>
    T parseField(string key, YAML::Node config) {
      if (config[key]) {
        return config[key].as<T>();
      }
      else {
        throw missingField(key, config);
      }
    }

};



vector<PeerGroup> parseYaml(const std::vector<YAML::Node> inputs) {
  vector<PeerGroup> groups;
  for (auto input : inputs) {
    groups.push_back(PeerGroup(input));
  }
  return groups;
}


class peerProfile {
public: peerProfile (string _ip, string _port):
    ip(_ip), port(_port) {}

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

  void setGlobals(list<YAML::Node> _globals) {
    globals = _globals;
  }

  double cpu;
  double mem;
  int  offer;
  vector<int> peers;
  vector<YAML::Node> params;     // Param list for ea peer
  list<YAML::Node> globals;
  list<YAML::Node> data;
};

class K3Job {
public:
  K3Job() {}

  K3Job (string _k3binary, YAML::Node _k3vars, bool _logging,
         int _total_peers, int _max_partitions, string _result_var,
         int _start_port, vector<PeerGroup> _groups)
  {
    k3binary       = _k3binary;
    k3vars         = _k3vars;
    logging        = _logging;
    total_peers    = _total_peers;
    max_partitions = _max_partitions;
    result_var     = _result_var;
    start_port     = _start_port;
    groups         = _groups;
  }

  K3Job(const K3Job& other) {
    k3binary       = other.k3binary;
    k3vars         = other.k3vars;
    logging        = other.logging;
    total_peers    = other.total_peers;
    max_partitions = other.max_partitions;
    result_var     = other.result_var;
    start_port     = other.start_port;
    groups         = other.groups;
  }

  string            k3binary;
  YAML::Node        k3vars;
  bool              logging;
  int               total_peers;
  int               max_partitions;
  string            result_var;
  int               start_port;
  vector<PeerGroup> groups;
  int               retries;

  string description() {
    stringstream s;
    s << "Task[" << k3binary << "(" << total_peers << "@" << start_port << ")]";
    return s.str();
  }
};

typedef tuple<list<int>, K3Job, shared_ptr<ostringstream>> ActiveJobState;

class K3Scheduler : public Scheduler {
public:
  K3Scheduler() {}

  virtual ~K3Scheduler() {}

  void run(K3Job job) {
    LOG(INFO) << "[RUN] Added pending task " << job.description() << endl;
    pendingJobs.push(job);
    ++numPendingJobs;
  }

  list<K3Job> getActiveTasks() {
    list<K3Job> r;
    for (auto entry : activeJobs) { r.push_back(std::get<1>(entry.second)); }
    return r;
  }

  int getNumPendingJobs() { return numPendingJobs; }

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

  string getTextAttr(const Offer& offer, const string& key) {
    for (int i = 0; i < offer.attributes_size(); i++) {
      const Attribute& attrib = offer.attributes(i);
      if (attrib.name().compare (key) == 0) {
        return attrib.text().value();
      }
    }

    throw std::runtime_error("Attribute " + key + "not found in offer");
  }

  // TODO: we currently assume we will be able to launch a job given a single set of offers (one call to this function).
  // In practice, we may need to keep track of offers between callbacks (taking into account offersRescinded)
  // For example, if we want to run on 4 machines but we are offered 2 at a time.
  virtual void resourceOffers(SchedulerDriver* driver, const vector<Offer>& offers)  {
    LOG(INFO) << "[RESOURCE OFFER] " << offers.size() << " offer(s)." << endl;

    if ( pendingJobs.empty() ) {
      for (const Offer& o: offers) {
        driver->declineOffer(o.id());
      }
      return;
    }

    // Launch task.
    K3Job next = pendingJobs.front();
    pendingJobs.pop();
    --numPendingJobs;

    LOG(INFO) << "Attempting to launch K3 Job " << jobsLaunched << endl;
    tuple<Status, list<int>> launchR = launchK3Job(driver, offers, next);
    if ( std::get<0>(launchR) == DRIVER_RUNNING ) {
      // Register task as active.
      activeJobs[jobsLaunched] = std::make_tuple(std::get<1>(launchR), next, make_shared<ostringstream>());
      LOG(INFO) << "Succesfully launched K3 Job " << jobsLaunched << endl;
      ++jobsLaunched;
    } else if ( --next.retries > 0 ) {
      // Retry up to a certain number of times.
      pendingJobs.push(next);
      ++numPendingJobs;
    }
    else {
      LOG(INFO) << "Failed to launch K3 Job " << jobsLaunched << endl;
    }
  }

  virtual void offerRescinded(SchedulerDriver* driver, const OfferID& offerId) {}

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)  {
    int taskId = lexical_cast<int>(status.task_id().value());

    if (status.state() == TASK_FINISHED) {
      LOG(INFO) << "Task " << taskId << " -> " << taskState[status.state()] << endl;
      cleanK3Job(driver, status, taskId);
    }

    else if (status.state() == TASK_RUNNING)  {
      LOG(INFO) << "Task " << taskId << " -> " << taskState[status.state()] << endl;
    }

    else {
      LOG(INFO) << "Task " << taskId << " -> " << taskState[status.state()] << endl;
      // Kill all tasks associated with the job
      cleanK3Job(driver, status, taskId, true);
    }

  }

  virtual void frameworkMessage(SchedulerDriver* driver,
                                const ExecutorID& executorId,
                                const SlaveID& slaveId, const string& data)
  {
    int id = std::stoi(executorId.value());
   
    // Log to the stream associated with the job
    map<int,int>::iterator it = executorJobs.find(id);
    if ( it != executorJobs.end() ) {
      map<int, ActiveJobState>::iterator actIt = activeJobs.find(it->second);
      if ( actIt != activeJobs.end() ) {
        ActiveJobState& activeTaskEntry = actIt->second;
	shared_ptr<ostringstream> s = std::get<2>(activeTaskEntry);
	*s << data;
	bool containsEndl = false;
        if (data.c_str()[data.length() - 1] == '\n') {
          containsEndl = true;
        }
	if (!containsEndl) {
	  *s << endl;
	}
     
      }
     
    }
  }

  virtual void slaveLost(SchedulerDriver* driver, const SlaveID& slaveId) {
    cout << "slave lost" << endl;
  }

  virtual void executorLost(SchedulerDriver* driver,
                            const ExecutorID& executorId,
                            const SlaveID& slaveId, int status)
  {
    cout << "Executor lost. Aborting" << endl;
    // TODO: stop remaining executors for the job, instead of stopping the driver.
    driver->stop();
  }

  virtual void error(SchedulerDriver* driver, const string& message) {
    cout << "error!" << endl;
  }

protected:
  void cleanK3Job(SchedulerDriver* driver, const TaskStatus& status, int executorId, bool abortJob = false)
  {
   
    // Update, and garbage collect active tasks.
    map<int,int>::iterator it = executorJobs.find(executorId);
    if ( it != executorJobs.end() ) {
      map<int, ActiveJobState>::iterator actIt = activeJobs.find(it->second);
      if ( actIt != activeJobs.end() ) {
        if ( abortJob ) {
          LOG(INFO) << "Asked to abort K3 Job " << it->second << ". All tasks will be killed."<< endl;
        }

        ActiveJobState& activeTaskEntry = actIt->second;
	auto executorIds = std::get<0>(activeTaskEntry);

	LOG(INFO) << "Cleaning Task " << executorId << ". Before cleanup there are " << executorIds.size() << " tasks left" << endl;
	for (int i : executorIds) {
          LOG(INFO) << "\tTask " << i << endl;
	}

        if ( abortJob || executorIds.size() == 1 ) {
          activeJobs.erase(it->second);
	  executorJobs.erase(executorId);
	  executorIds.remove(executorId);
	 
	  if ( abortJob ) {
	    // Kill all tasks associated with the job
            for (int id : executorIds) {
              TaskID t;
	      t.set_value(stringify(id));
	      LOG(INFO) << "Killing Task " << id << "." << endl;
	      driver->killTask(t);
	    }
	  }
	 
          LOG(INFO) << "K3 Job " << it->second << " -> " << taskState[status.state()] << endl;
	  LOG(INFO) << "Job " << it->second << " output: " << endl;
	  LOG(INFO) << std::get<2>(activeTaskEntry)->str() << endl;
        } else {
	  list<int> executorIds = std::get<0>(activeTaskEntry);
	  executorIds.remove(executorId);
          activeJobs[it->second] = std::make_tuple(executorIds, std::get<1>(activeTaskEntry), std::get<2>(activeTaskEntry));
	  executorJobs.erase(executorId);
        }
      }
    }
  }

  tuple<Status, list<int>> launchK3Job(SchedulerDriver* driver,
                                 const vector<Offer>& offers,
                                 const K3Job& k3task)
  {
    Status launchStatus;
    list<int> executorIds;

    // Iterate through each resource offer (1 offer per slave with available resources)
    //    Offer -> Vector of resources (cpu, mem, disk, etc)
    vector<int> cpusUsed = vector<int>(offers.size());
    vector<list<YAML::Node>> globals(offers.size());
    vector<list<YAML::Node>> data(offers.size());
    for (const PeerGroup& pg : k3task.groups) {
      int assignedPeers = 0;

      for (size_t i=0; i < offers.size(); i++) {
        const Offer& offer = offers[i];
        double cpus = getCpus(offer);
        if (cpusUsed[i] == cpus) { continue; }

        if (pg.server_ != "") {
          string server = offer.hostname();
          if (pg.server_ != server) { continue; }
        }

        if (pg.server_group_ != "") {
          string server_group = getTextAttr(offer, "cat");
          if (pg.server_group_ != server_group) { continue; }
        }

        int numPeers = pg.peers_;
        if (cpus < numPeers) { numPeers = cpus; }
        if (pg.peers_per_host_ != 0 && pg.peers_per_host_ < numPeers) { numPeers = pg.peers_per_host_; }

	cpusUsed[i] += numPeers;
        assignedPeers += numPeers;

        for (int j = 0; j < numPeers; j++) {
          globals[i].push_back(pg.globals_);
          data[i].push_back(pg.data_);
        }

        if (assignedPeers >= pg.peers_) { break; }
      }

      if (assignedPeers < pg.peers_) {
        throw std::runtime_error("Failed to assign all peers in group: " + YAML::Dump(pg.yaml_));
      }
    }

    // Reject any unused offers
    for (size_t i =0; i < cpusUsed.size(); i++) {
      if ( cpusUsed[i] == 0) {
        driver->declineOffer(offers[i].id());
      }
    }
    LOG(INFO) << "Successfully allocated all peers: " << endl;

    int peersAssigned = 0;
    for (size_t i=0; i < offers.size(); i++) {
      if (globals[i].size() == 0) { continue; }

      hostProfile profile;
      profile.cpu = globals[i].size();
      profile.mem = getMem(offers[i]);
      profile.offer = i;
      profile.globals = globals[i];
      profile.data = data[i];

      for (size_t p = 0; p < globals[i].size(); p++)  {
        int peerId = peersAssigned++;

        // TODO: PORT management
        string port = stringify(k3task.start_port + peerId);
        profile.addPeer(peerId);

        peerProfile peer (ip_addr[offers[i].hostname()], port);
        peerList.push_back(peer);
      }

      hostList[offers[i].hostname()] = profile;
    }

    // Build the Peers list
    vector<YAML::Node> peerNodeList;
    for (auto &peer : peerList) {
      YAML::Node peerNode;
      peerNode["addr"] = peer.getAddr();
      peerNodeList.push_back(peerNode);
    }

    for (auto pair : ip_addr) {
      if (pair.second == peerList[0].ip) {
        LOG(INFO) << "Automatically picked master:" << pair.first << endl;
      }
    }

    for (auto &host : hostList)  {
      string hostname = host.first;
      hostProfile profile = host.second;

      //  Build parameter
      YAML::Node hostParams;
      map<string, string> mountPoints;

      if (k3task.logging) {
        hostParams["logging"] = "-l INFO";
      }

      hostParams["binary"]        = k3task.k3binary;
      hostParams["totalPeers"]    = peerList.size();
      hostParams["peerStart"]     = profile.peers.front();
      hostParams["peerEnd"]       = profile.peers.back();
      hostParams["maxPartitions"] = k3task.max_partitions;

      for (auto &p : profile.peers) {
        hostParams["me"].push_back(peerList[p].getAddr());
      }
      for (auto &p : peerNodeList) {
        hostParams["peers"].push_back(p);
      }

      for (auto& it : profile.globals) {
        YAML::Node g;
        for (YAML::const_iterator var = it.begin(); var != it.end(); var++) {
          YAML::Node data = var->second;
          string key = var->first.as<string>();

          if (data.Type() == YAML::NodeType::Scalar && data.as<string>() == "auto") {
            g[key] = peerList[0].getAddr();
            if (key == "master") {
              hostParams["master"] = peerList[0].getAddr();
            }
          } else {
            g[key] = data;
          }
        }
        hostParams["globals"].push_back(g);
      }

      hostParams["data"] = profile.data;
      mountPoints["/local/data"] = "/local/data";

      //  else if (key == "data") {
      //    YAML::Node data_files = var->second;
      //    hostParams["dataFiles"] = data_files;
      //    mountPoints["/local/data"] = "/local/data";
      //  }

      if (k3task.result_var != "") {
        hostParams["resultVar"] = k3task.result_var;
      }

      ExecutorInfo executor = makeExecutor(k3task.k3binary, hostParams, mountPoints, executorsLaunched);

      TaskInfo task;
      task.set_name(k3task.k3binary + "@" + hostname);
      task.mutable_task_id()->set_value(stringify(executorsLaunched));
      task.mutable_slave_id()->MergeFrom(offers[profile.offer].slave_id());
      task.mutable_executor()->MergeFrom(executor);

      // Pass parameters onto the host
      task.set_data(YAML::Dump(hostParams));

      Resource* resource;

      resource = task.add_resources();
      resource->set_name("cpus");
      resource->set_type(Value::SCALAR);
      resource->mutable_scalar()->set_value(profile.cpu);

      resource = task.add_resources();
      resource->set_name("mem");
      resource->set_type(Value::SCALAR);
      resource->mutable_scalar()->set_value(profile.mem);

      vector<TaskInfo> tasks;  // Now running 1 task per slave
      tasks.push_back(task);

      LOG(INFO) << " Launching Peers # " << stringify(profile.peers.front()) << " - " << stringify (profile.peers.back()) << " on " << hostname << endl;
      launchStatus = driver->launchTasks(offers[profile.offer].id(), tasks);
      if ( launchStatus == DRIVER_STOPPED  || launchStatus == DRIVER_ABORTED ) { break; }

      executorJobs[executorsLaunched] = jobsLaunched;
      executorIds.push_back(executorsLaunched);
      ++executorsLaunched;
    }

    return std::make_tuple(launchStatus, executorIds);
  }

private:
  // const ExecutorInfo executor;
  int peersAssigned = 0;
  int executorsLaunched = 0;
  int jobsLaunched = 0;

  string runpath;
  string fileServer;
  map<string, hostProfile> hostList;
  vector<peerProfile> peerList;

  queue<K3Job> pendingJobs;
  int numPendingJobs;

  map<int, ActiveJobState> activeJobs;
  // Map from executorID to jobID
  map<int, int> executorJobs;
};

// A proxy class for MesosSchedulerDriver
class K3SchedulerDriver {
public:
  K3SchedulerDriver() { initialize(); }

  void initialize()
  {
    string master = MASTER;

    framework.set_user(""); // Have Mesos fill in the current user.
    framework.set_name("K3-" + currTime());
    framework.mutable_id()->set_value("K3-" + currTime());

    // FROM: Example Frame, left unchanged for adding Creds/ChckPts, etc..
    if (os::hasenv("MESOS_CHECKPOINT")) {
      cout << "Enabling checkpoint for the framework" << endl;
      framework.set_checkpoint(true);
    }

    if (os::hasenv("MESOS_AUTHENTICATE")) {
      cout << "Enabling authentication for the framework" << endl;

      if (!os::hasenv("DEFAULT_PRINCIPAL")) {
        EXIT(1) << "Expecting authentication principal in the environment";
      }

      if (!os::hasenv("DEFAULT_SECRET")) {
        EXIT(1) << "Expecting authentication secret in the environment";
      }

      credential.set_principal(getenv("DEFAULT_PRINCIPAL"));
      credential.set_secret(getenv("DEFAULT_SECRET"));

      framework.set_principal(getenv("DEFAULT_PRINCIPAL"));

      driver = std::make_shared<MesosSchedulerDriver>(&scheduler, framework, master, credential);
    } else {
      framework.set_principal("k3-framework");
      driver = std::make_shared<MesosSchedulerDriver>(&scheduler, framework, master);
    }
  }

  void runJob(K3Job job) { scheduler.run(job); }

  // Proxy methods for scheduler driver control.
  Status start() { return driver->start(); }
  Status stop(bool failover = false) { return driver->stop(failover); }
  Status abort() { return driver->abort(); }
  Status join() { return driver->join(); }
  Status run() { return driver->run(); }

protected:
  shared_ptr<MesosSchedulerDriver> driver;
  K3Scheduler scheduler;
  FrameworkInfo framework;
  Credential credential;
};

// A web-based K3+Mesos scheduler driver.
class K3SDriverHTTP : public K3SchedulerDriver {
public:
  K3SDriverHTTP(int port, int threads)
  {
    server = make_shared<Server<HTTP>>(port, threads);
    initializeRoutes();
    startServices();
  }

  void startServices() {
    http_server_thread = std::make_shared<thread>([this](){ this->server->start(); });
    mesos_driver_thread = std::make_shared<thread>([this](){
      this->run();
      this->stop();
    });
  }

  void waitForServices() {
    if ( http_server_thread )  {
      http_server_thread->join();
      this->stop();
    }
    if ( mesos_driver_thread && mesos_driver_thread->joinable() ) { mesos_driver_thread->join(); }
  }

  // Set up routes handled by the HTTP server.
  void initializeRoutes() {
    if ( server ) {

      //POST-example for the path /string, responds the posted string
      server->resource["^/runtask/?$"]["POST"]=[](ostream& response, auto request) {
          stringstream ss;
          request->content >> ss.rdbuf();
          string content = ss.str();

          response << "HTTP/1.1 200 OK\r\nContent-Length: " << content.length() << "\r\n\r\n" << content;
      };

      //GET-example for the path /info
      //Responds with request-information
      server->resource["^/tasks/?$"]["GET"]=[](ostream& response, auto request) {
        stringstream content_stream;
        content_stream << "<h1>Request:</h1>";
        content_stream << request->method << " " << request->path << " HTTP/" << request->http_version << "<br>";
        for(auto& header: request->header) {
            content_stream << header.first << ": " << header.second << "<br>";
        }

        response <<  "HTTP/1.1 200 OK\r\n\r\n" << content_stream.rdbuf();
      };
    }
  }

private:
  shared_ptr<Server<HTTP>> server;
  shared_ptr<thread> http_server_thread;
  shared_ptr<thread> mesos_driver_thread;
};

int main(int argc, char** argv) {
  string master = MASTER;
  string k3binary;
  string k3args_json;
  string k3args_yaml;
  string result_var;
  YAML::Node k3vars;
  int max_partitions = 0;
  int total_peers = (argc == 2) ? atoi(argv[1]) : 4;
  int start_port = 40000;
  bool logging = false;
  bool ktrace = false;

  //  Parse Command Line options
  namespace po = boost::program_options;
  vector <string> optionalVars;
  vector <PeerGroup> peerGroups;

  po::options_description desc("K3 Run options");
  desc.add_options()
    ("program", po::value<string>(&k3binary)->required(), "K3 executable program filename")
    ("yaml", po::value<string>(&k3args_yaml)->required(), "YAML encoded input file")
    ("max_partitions", po::value<int>(&max_partitions), "Hard Limit number of data file partitions")
    ("logging,l", po::value<bool>(&logging), "Toggle K3 Logging (global environment)")
    ("ktrace,k", po::value<bool>(&logging), "Toggle KTrace (log globals and messages to JSON for use with Postgresql) ")
    ("help,h", "Print help message");

  po::positional_options_description positionalOptions;
    positionalOptions.add("program", 1);
    positionalOptions.add("yaml", 1);

  po::variables_map vm;

  try {
    po::store(po::command_line_parser(argc, argv).options(desc)
                  .positional(positionalOptions).run(), vm);
    if (vm.count("help") || vm.empty()) {
      cout << "K3 Distributed program framework backed by Mesos cluser" << endl << endl;
      cout << "Usage: " << endl;
      cout << "       k3Scheduler <k3-Binary> <yaml-topology> [options]" << endl << endl;
      cout << desc << endl;
      return 0;
    }

    po::notify(vm);

    if (vm.count("yaml")) {
      string buf;
      std::ostringstream contents;
      std::ifstream instream(k3args_yaml);
      while ( std::getline(instream, buf) ) {
        contents << buf << "\n";
      }
      std::vector<YAML::Node> v  = YAML::LoadAll (contents.str());
      peerGroups = parseYaml(v);
      LOG(INFO) << "Input YAML: " << endl;
      for (auto y : peerGroups) {
        LOG(INFO) << endl << "---" << endl <<YAML::Dump(y.yaml_) << endl;
      }
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

  K3Job task(k3binary, k3vars, logging, total_peers, max_partitions, result_var, start_port, peerGroups);
  K3SDriverHTTP drv(8080, 4);
  drv.runJob(task);
  drv.waitForServices();
  return 0;
}

ExecutorInfo makeExecutor (string programBinary,
                           YAML::Node hostParams,
                           map<string, string> mounts, int id)
{
    string fileServer = FILE_SERVER;

    // CREATE The DOCKER Executor
    ExecutorInfo executor;
    executor.mutable_executor_id()->set_value(stringify(id));


    // Sets executor name to program name to pass along to slaves for execution
    executor.set_name(programBinary);
    executor.set_data(YAML::Dump(hostParams));

    // executor.set_source("K3");    // not necessary to set this,
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
    //  executor.mutable_command()->set_shell(false);
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
