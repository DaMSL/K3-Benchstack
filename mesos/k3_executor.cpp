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

#include <iostream>
#include <string>
#include <unistd.h>
#include <dirent.h>

#include <mesos/executor.hpp>

#include <stout/duration.hpp>
#include <stout/os.hpp>

#include <yaml-cpp/yaml.h>


using namespace mesos;
using namespace std;

class KDExecutor : public Executor
{
public:

  virtual ~KDExecutor() {}

  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo)
  {
	driver->sendFrameworkMessage("Executor Registered at " + slaveInfo.hostname());
	host_name= slaveInfo.hostname();
	localPeerCount = 0;
	totalPeerCount = 0;
  }

  virtual void reregistered(ExecutorDriver* driver, const SlaveInfo& slaveInfo)   {
    cout << "Re-registered executor on " << slaveInfo.hostname() << endl;
	host_name= slaveInfo.hostname();
  }

  virtual void disconnected(ExecutorDriver* driver) {}

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)    {
	localPeerCount++;
	
	
	driver->sendFrameworkMessage("Executor launching task on " + host_name);
    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_state(TASK_RUNNING);
    driver->sendStatusUpdate(status);

    //-------------  START TASK OPERATIONS ----------
	cout << "Running K3 Program: " << task.name() << endl;
	string k3_cmd;
	
	using namespace YAML;
	
	Node hostParams = Load(task.data());
	Node peerParams;
	Node peers;
//	vector<Node> peers;
	
	cout << "WHAT I RECEIVED\n----------------------\n";
	cout << Dump(hostParams);
	cout << "\n---------------------------------\n";
	
	k3_cmd = "$MESOS_SANDBOX/" + hostParams["binary"].as<string>();
	if (hostParams["logging"]) {
		k3_cmd += " -l INFO";
	}
	
	
	string datavar, datapath;
	string datapolicy = "default";
	int peerStart = 0;
	int peerEnd = 0;
	
	for (const_iterator param=hostParams.begin(); param!=hostParams.end(); param++)  {
		string key = param->first.as<string>();
//		cout << " PROCESSING: " << key << endl;
		if (key == "logging" || key == "binary" || 
			key == "server" || key == "server_group") {
			continue;
		}
		else if (key == "peers") {
			peerParams["peers"] = hostParams["peers"];
		}
		else if (key == "me") {
			Node meList = param->second;
			YAML::Emitter emit;
			emit << YAML::Flow << meList;
			for (std::size_t i=0; i<meList.size(); i++)  {
				peers.push_back(meList[i]);
			}
		}
		else if (key == "datavar") {
			datavar = param->second.as<string>();
		}
		else if (key == "datapath") {
			datapath = "{path: " + param->second.as<string>() + "}";
		}
		else if (key == "datapolicy") {
			datapolicy = param->second.as<string>();
		}
		else if (key == "totalPeers")  {
			totalPeerCount = param->second.as<int>();
		}
		else if (key == "peerStart") {
			peerStart = param->second.as<int>();
		} 
		else if (key == "peerEnd") {
			peerEnd = param->second.as<int>();
		}
		else {
//			string value = i->second.as<string>();
			peerParams[key] = param->second;
		}
	}
	
	// DATA ALLOCATION *
		// TODO: Convert to multiple input dirs
	vector<string> dataFile;
	vector<string> peerFiles[peers.size()];

	if (datavar != "") {

		// 1. GET DIR LIST IN datavar
		DIR *datadir = NULL;
		datadir = opendir("/mnt/data");
		struct dirent *srcfile = NULL;
		
		while (true) {
			srcfile = readdir(datadir);
			if (srcfile == NULL) {
				break;
			}
			cout << "FILE  " << srcfile->d_name << ":  ";
			if (srcfile->d_type == DT_REG) {
				string filename = srcfile->d_name;
				dataFile.push_back("/mnt/data/" + filename);
				cout << "Added -> " << filename;
			}
			cout << endl;
		}
		closedir(datadir);

		int numfiles = dataFile.size();

		sort (dataFile.begin(), dataFile.end());
		
		int p_start = 0;
		int p_end = numfiles;

		int p_total = peers.size();

		if (datapolicy == "global") {
			int taskNum = atoi(task.task_id().value().c_str());
			p_start = (numfiles / totalPeerCount) * peerStart;
			p_end = (numfiles / totalPeerCount) * (peerEnd+1);
			p_total = totalPeerCount;
			cout << ("Global files s=" + stringify(p_start) + " e=" + stringify(p_end) + " t=" + stringify(p_total)) << endl;
		}
		for (int filenum = p_start; filenum < p_end; filenum++) {
			int peer = floor((((p_total)*1.0*filenum) / numfiles)) - peerStart;
			cout << "  Peer # " << peer << " : [" << filenum << "] " << dataFile[filenum] << endl;
			peerFiles[peer].push_back(dataFile[filenum]);
		}
		


		// 2. divide up dir list in datavar
		// 3. create a vector<string> for each 
//		for (std::size_t i=0; i<peers.size(); i++)  {
//			dataFile.push_back("/mnt/data/rankings0000");
//		}
	}
	
//	cout << "   [K3 CMD:] " << task.data().c_str() << endl;
	cout << "BUILDING PARAMS FOR PEERS" << endl;
	
	for (std::size_t i=0; i<peers.size(); i++)  {
		YAML::Node thispeer = peerParams;
		YAML::Node me = peers[i]; 
		thispeer["me"] = me;
		if (thispeer[datavar]) {
			thispeer.remove(datavar);
		}
		if (datavar != "")  {
			for (auto &f : peerFiles[i]) {
				Node src;
				src["path"] = f;
				thispeer[datavar].push_back(src);
			}
		}
		// ADD DATA SOURCE DIR HERE
		YAML::Emitter emit;
		emit << YAML::Flow << thispeer;
		string param = emit.c_str();
//		cout << "PARAM: " << param << endl;
		k3_cmd += " -p '" + param + "'";
	}
	
	cout << "FINAL COMMAND: " << k3_cmd << endl;

	// Currently, just call the K3 executable with the generated command line from task.data()
	int k3 = system(k3_cmd.c_str());
	//cout << "  K3 System call returned: " << stringify(k3) << endl;
	if (k3 == 0) {
		status.set_state(TASK_FINISHED);
		cout << "Task " << task.task_id().value() << " Completed!" << endl;
	}
	else {
		status.set_state(TASK_FAILED);
		cout << "K3 Task " << task.task_id().value() << " returned error code: " << k3 << endl;
	}
	//-------------  END OF TASK  -------------------

    driver->sendStatusUpdate(status);
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {
	  	  driver->sendFrameworkMessage("Executor " + host_name+ " KILLING TASK");
}

  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {
	  driver->sendFrameworkMessage(data);
  }

  virtual void shutdown(ExecutorDriver* driver) {
  	driver->sendFrameworkMessage("Executor " + host_name+ "SHUTTING DOWN");
  }

  virtual void error(ExecutorDriver* driver, const string& message) {
	  driver->sendFrameworkMessage("Executor " + host_name+ " ERROR");
}
  
private:
  string host_name;
  int state;
  int localPeerCount;
  int totalPeerCount;
  int tasknum;
};


int main(int argc, char** argv)
{
	KDExecutor executor;
	MesosExecutorDriver driver(&executor);
    return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
