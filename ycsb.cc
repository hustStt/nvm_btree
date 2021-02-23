#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <future>
#include "include/common_time.h"
#include "src/single_pmdk.h"
#include "src/single_btree.h"
#include "include/ycsb/ycsb-c.h"
#include "random.h"

using namespace std;

#define LOGPATH "/mnt/pmem0/log_persistent"
#define PATH "/mnt/pmem0/ycsb"
#define NODEPATH   "/mnt/pmem0/persistent"

const uint64_t NVM_LOG_SIZE = 10 * (1ULL << 30);
const uint64_t NVM_NODE_SIZE = 1 * (1ULL << 30);

const char *workloads[] = {
  // "workloada_insert_0.spec",
  // "workloada_insert_10.spec",
  // "workloada_insert_20.spec",
  // "workloada_insert_50.spec",
  // "workloada_insert_80.spec",
  // "workloada_insert_100.spec",
  // "workload_read.spec",
   "workload_insert.spec",
   "workloada.spec",
   "workloadb.spec",
   "workloadc.spec",
   "workloadd.spec",
   "workloade.spec",
   "workloadf.spec",
};

#define ArrayLen(arry) (sizeof(arry) / sizeof(arry[0]))

class FastFairDb : public ycsbc::KvDB {
public:
    FastFairDb(): tree_(nullptr) {}
    FastFairDb(btree *tree): tree_(tree) {}
    virtual ~FastFairDb() {
      mybt->exitBtree();
    }
    void Init()
    {
      mybt = MyBtree::getInitial(PATH);
      tree_ = mybt->getBt();
    }

    void Info()
    {
    }

    void Close() { 

    }
    int Put(uint64_t key, uint64_t value) 
    {
        tree_->btreeInsert(key, (char *)value);
        return 1;
    }
    int Get(uint64_t key, uint64_t &value)
    {
        value = (uint64_t)tree_->btreeSearch(key);
        return 1;
    }
    int Update(uint64_t key, uint64_t value) {
        //tree_->btreeDelete(key);
        tree_->btreeUpdate(key, (char *)value);
        return 1;
    }
    int Scan(uint64_t start_key, int len, std::vector<std::pair<uint64_t, uint64_t>>& results) 
    {
        tree_->btreeSearchRange(start_key, UINT64_MAX, results, len);
        return 1;
    }
private:
    btree *tree_;
    MyBtree *mybt;
};

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
int LoadWorkLoad(utils::Properties &props, string &workload);

int YCSB_Run(ycsbc::KvDB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  ycsbc::KvClient<ycsbc::KvDB> client(db, *wl);
  utils::ChronoTimer timer;
  int oks = 0;
  if(is_loading) {
    timer.Start();
  }
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
    if(i%100000 == 0) {
      // std::cerr << "Trans: " << i << "\r";
      if(is_loading) {
        auto duration = timer.End<std::chrono::nanoseconds>();
        std::cout << "op " << i << " :average load latency: " << 1.0 * duration / 100000 << " ns." <<std::endl;
      }
      // std::cout << "average load latency: " << duration << std::endl;
      timer.Start();
      db->PrintStatic();
    }
  }
  if(is_loading) {
    auto duration = timer.End();
    std::cout << "op " << num_ops <<  " :average load latency: " << 1.0 * duration / 10000<< std::endl;
  }
  std::cerr << "Trans: " << num_ops <<std::endl;
  return oks;
}

int main(int argc, const char *argv[])
{
    ycsbc::KvDB *db = nullptr;
    utils::Properties props;
    ycsbc::CoreWorkload wl;
    string workdloads_dir = ParseCommandLine(argc, argv, props);
    string dbName = props["dbname"];
    const int num_threads = 1;//stoi(props.GetProperty("threadcount", "1"));
    // Loads data
    vector<future<int>> actual_ops;
    // Loads data
    int total_ops = 0;
    int sum = 0;

    std::cout << "YCSB test:" << dbName << std::endl;
    if(dbName == "fastfair") {
      db = new FastFairDb();
    }
    db->Init();

    if(AllocatorInit(LOGPATH, NVM_LOG_SIZE, NODEPATH, NVM_NODE_SIZE) < 0) {
        print_log(LV_ERR, "Initial allocator failed");
        return 0;
    }

    {
      string workload = workdloads_dir + "/" + workloads[0];
      LoadWorkLoad(props, workload);
      wl.Init(props);
      // Loads data
      total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
      utils::Timer<double> timer;
      timer.Start();
      for (int i = 0; i < 1; ++i) {
          actual_ops.emplace_back(async(launch::async,
              YCSB_Run, db, &wl, total_ops / num_threads, true));
      }
      assert((int)actual_ops.size() == num_threads);

      for (auto &n : actual_ops) {
          assert(n.valid());
          sum += n.get();
      }
      double duration = timer.End();
      cout << "# Loading_records:\t" << sum << " throughput (KTPS)" <<endl;
      cout << props["dbname"] << "\tLoad_thread:" << '\t' << 1 << '\t';
      cout << total_ops / duration / 1000 << endl << endl;
    }
    db->Info();
    for(size_t i = 0; i < ArrayLen(workloads); i ++) {
      // cout << "Loads[" << i << "]: " << workloads[i] << endl;
      string workload = workdloads_dir + "/" + workloads[i];
      LoadWorkLoad(props, workload);
      wl.Reload(props);
      // Peforms transactions
      actual_ops.clear();
      total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
      cerr << props["dbname"] << " start \t" << workloads[i] << "\t: ops " << total_ops << endl;
      utils::Timer<double> timer;
      db->Begin_trans();
      timer.Start();
      for (int i = 0; i < num_threads; ++i) {
          actual_ops.emplace_back(async(launch::async,
              YCSB_Run, db, &wl, total_ops / num_threads, false));
      }
      assert((int)actual_ops.size() == num_threads);

      sum = 0;
      for (auto &n : actual_ops) {
          assert(n.valid());
          sum += n.get();
      }
      double duration = timer.End();
      cout << "# Transaction throughput (KTPS)" << endl;
      cout << props["dbname"] << '\t' << workloads[i] << '\t' << num_threads << '\t';
      cout << total_ops / duration / 1000 << endl << endl;
      db->Info();
    }
    delete db;
    AllocatorExit();
    return 0;
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      // ifstream input(argv[argindex]);
      // try {
      //   props.Load(input);
      // } catch (const string &message) {
      //   cout << message << endl;
      //   exit(0);
      // }
      // input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

int LoadWorkLoad(utils::Properties &props, string &workload) {
  ifstream input(workload);
  try {
    props.Load(input);
  } catch (const string &message) {
    cout << message << endl;
    exit(0);
  }
  input.close();
  return 0;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile directory: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}