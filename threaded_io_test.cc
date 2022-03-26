#include "TROOT.h"
#include "TVirtualStreamerInfo.h"
#include "TObject.h"
#include <iostream>
#include <vector>
#include <string>
#include <atomic>
#include <iomanip>
#include <cmath>

#include "CLI11.hpp"

#include "outputerFactoryGenerator.h"
#include "sourceFactoryGenerator.h"

#include "Lane.h"

#include "tbb/task_group.h"
#include "tbb/global_control.h"
#include "tbb/task_arena.h"
#include "H5Timing.h"
#include "multidataset_plugin.h"

extern int max_batch_size;
extern int hdf_method;
extern int total_n_events;

namespace {
  std::pair<std::string, std::string> parseCompound(std::string_view iArg) {
    std::string sArg(iArg);
    auto found = sArg.find('=');
    auto next = found;
    if(found != std::string::npos) {
      return std::pair(sArg.substr(0,found), sArg.substr(found+1));
    }
    return std::pair(sArg, std::string());
  }
}

int main(int argc, char* argv[]) {
  MPI_Init(&argc, &argv);

#ifdef H5_TIMING_ENABLE
  init_timers();
#endif

  init_multidataset();

  using namespace cce::tf;

  CLI::App app{"test different I/O systems under threading"};

  std::string sourceConfig;
  app.add_option("-s,--source",sourceConfig,"configure Source")->required();
  
  int parallelism = tbb::this_task_arena::max_concurrency();
  app.add_option("-t,--num-threads", parallelism, "number of threads to use.\nDefault is all cores on the machine.");

  bool useIMT = false;
  app.add_option("--use-IMT", useIMT, "Use ROOT's Implicit MultiThreading.\nDefault is false.");

  unsigned int nLanes = parallelism;
  app.add_option("-l,--num-lanes", nLanes, "Number of concurrently processing event Lanes.\nDefault is number of threads.");

  unsigned long long nEvents = std::numeric_limits<unsigned long long>::max();
  app.add_option("-n,--num-events", nEvents, "Number of events to process.\nDefault is max value.");

  std::string outputerConfig="DummyOutputer";
  app.add_option("-o,--outputer", outputerConfig, "configure Outputer.\nDefault is 'DummyOutputer'.");

  double scale = -1.;
  app.add_option("--scale", scale, "Scale to use when converting data product size to wait time. A value less than 1 turns off this feature. \nDefault is -1.");

  CLI11_PARSE(app, argc, argv);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, parallelism);

  //Tell Root we want to be multi-threaded
  if(useIMT) {
    ROOT::EnableImplicitMT(parallelism);
  } else {
    ROOT::EnableThreadSafety();
  }
  //When threading, also have to keep ROOT from logging all TObjects into a list
  TObject::SetObjectStat(false);
  
  //Have to avoid having Streamers modify themselves after they have been used
  TVirtualStreamerInfo::Optimize(false);

  std::vector<Lane> lanes;

  std::function<std::unique_ptr<OutputerBase>(unsigned int)> outFactory;
  {
    auto [outputType, outputInfo] = parseCompound(outputerConfig);
    outFactory = outputerFactoryGenerator(outputType, outputInfo);
    if(not outFactory) {
      std::cout <<"unknown output type "<<outputType<<std::endl;
      return 1;
    }
  }

  auto [sourceType, sourceOptions] = parseCompound(sourceConfig);
  auto sourceFactory = sourceFactoryGenerator(sourceType, sourceOptions);
  if(not sourceFactory) {
    std::cout <<"unknown source type "<<sourceType<<std::endl;
    return 1;
  }
 
  {
    //warm up the system by processing 1 event 
    tbb::task_arena arena(1);
    auto out = outFactory(1);
    if(not out) {
      std::cout <<"failed to create outputer\n";
      return 1;
    }
    auto source =sourceFactory(1,1);
    if(not source) {
      std::cout <<"failed to create source\n";
      return 1;
    }
    Lane lane(0, source.get(), 0);
    out->setupForLane(0, lane.dataProducts());
    auto pOut = out.get();
    std::cout <<"begin warmup"<<std::endl;
    arena.execute([&lane,pOut]() {
        tbb::task_group group;
        std::atomic<long> ievt{0};
	std::atomic<unsigned int> count{0};
        group.run([&]() {
            lane.processEventsAsync(ievt, group, *pOut, AtomicRefCounter(count));
          });
        group.wait();
      });
  }
  std::cout <<"finished warmup"<<std::endl;

  auto out = outFactory(nLanes);
  auto source = sourceFactory(nLanes, nEvents);
  lanes.reserve(nLanes);
  for(unsigned int i = 0; i< nLanes; ++i) {
    lanes.emplace_back(i, source.get(), scale);
    out->setupForLane(i, lanes.back().dataProducts());
  }

  std::atomic<long> ievt{0};
  
  tbb::task_arena arena(parallelism);

  decltype(std::chrono::high_resolution_clock::now()) start;
  auto pOut = out.get();
  arena.execute([&lanes, &ievt, pOut, &start]() {
    std::atomic<unsigned int> nLanesWaiting{ 0 };
    std::vector<tbb::task_group> groups(lanes.size());
    start = std::chrono::high_resolution_clock::now();
    auto itGroup = groups.begin();
    {
      AtomicRefCounter laneCounter(nLanesWaiting);
      for(auto& lane: lanes) {
        auto& group = *itGroup;
        group.run([&, laneCounter]() {lane.processEventsAsync(ievt,group, *pOut,laneCounter);});
        ++itGroup;
      }
    }
    do {
      for(auto& group: groups) {
	group.wait();
      }
    } while(nLanesWaiting != 0);
    //be sure all groups have fully finished
    for(auto& group: groups) {
      group.wait();
    }
  });

  std::chrono::microseconds eventTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-start);

  //NOTE: each lane will go 1 beyond the # events so ievt is more then the # events
  std::cout <<"----------"<<std::endl;
  std::cout <<"Source "<<sourceConfig<<"\n"
            <<"Outputer "<<outputerConfig<<"\n"
	    <<"# threads "<<parallelism<<"\n"
	    <<"# concurrent events "<<nLanes <<"\n"
	    <<"time scale "<<scale<<"\n"
	    <<"use ROOT IMT "<< (useIMT? "true\n":"false\n");
  std::cout <<"Event processing time: "<<eventTime.count()<<"us"<<std::endl;
  std::cout <<"number events: "<<ievt.load() -nLanes<<std::endl;
  std::cout <<"----------"<<std::endl;

  source->printSummary();
  out->printSummary();

  finalize_multidataset();
#ifdef H5_TIMING_ENABLE
  finalize_timers();
#endif

  MPI_Finalize();
}
