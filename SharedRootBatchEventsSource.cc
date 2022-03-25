#include "SharedRootBatchEventsSource.h"
#include "SourceFactory.h"
#include "Deserializer.h"
#include "UnrolledDeserializer.h"

#include "TClass.h"

using namespace cce::tf;

SharedRootBatchEventsSource::SharedRootBatchEventsSource(unsigned int iNLanes, unsigned long long iNEvents, std::string const& iName) :
  SharedSourceBase(iNEvents),
  file_{TFile::Open(iName.c_str())},
  pEventIDs_(&eventIDs_),
  pOffsetsAndBuffer_(&offsetsAndBuffer_),
  readTime_{std::chrono::microseconds::zero()}
{

  //gDebug = 3;

  if(not file_) {
    std::cout <<"unknown file "<<iName<<std::endl;
    throw std::runtime_error("uknown file");
  }

  eventsTree_ = file_->Get<TTree>("Events");
  if(not eventsTree_) {
    std::cout <<"no Events TTree in file "<<iName<<std::endl;
    throw std::runtime_error("no Events TTree");
  }
  eventsBranch_ = eventsTree_->GetBranch("offsetsAndBlob");
  eventsBranch_->SetAddress(&pOffsetsAndBuffer_);
  if( not eventsBranch_) {
    std::cout <<"no 'offsetsAndBlob' TBranch in 'Events' TTree in file "<<iName<<std::endl;
    throw std::runtime_error("no 'offsetsAndBlob' TBranch");
  }

  idBranch_ = eventsTree_->GetBranch("EventIDs");
  idBranch_->SetAddress(&pEventIDs_);
  if(not idBranch_) {
    std::cout <<"no 'EventIDs' TBranch in 'Events' TTree in file "<<iName<<std::endl;
    throw std::runtime_error("no 'EventIDs' TBranch");
  }
   
  auto meta = file_->Get<TTree>("Meta");
  if(not meta) {
    std::cout <<"no 'Meta' TTree in file "<<iName<<std::endl;
    throw std::runtime_error("no 'Meta' TTree");
  }

  std::vector<std::pair<std::string, std::string>> typeAndNames;
  int objectSerializationUsed;
  std::string compression;

  {
    auto typeAndNamesBranch = meta->GetBranch("DataProducts");
    auto pTemp = &typeAndNames;
    typeAndNamesBranch->SetAddress(&pTemp);
    typeAndNamesBranch->GetEntry(0);

    //std::cout <<"typeAndNames.size() "<<typeAndNames.size()<<std::endl;
    //for(auto const& tn: typeAndNames) {
    //  std::cout <<"  "<<tn.first<<" "<<tn.second<<std::endl;
    //}
    
  }
  {
    auto serializerBranch = meta->GetBranch("objectSerializationUsed");
    serializerBranch->SetAddress(&objectSerializationUsed);
    serializerBranch->GetEntry(0);

    //std::cout <<"serialization "<<objectSerializationUsed<<std::endl;
  }
  {
    auto compressionBranch = meta->GetBranch("compressionAlgorithm");
    auto pTemp = &compression;
    compressionBranch->SetAddress(&pTemp);
    compressionBranch->GetEntry(0);
    //std::cout <<"compressionAlgorithm "<<compression<<std::endl;
  }

  assert(objectSerializationUsed == static_cast<int>(pds::Serialization::kRoot) or 
         objectSerializationUsed == static_cast<int>(pds::Serialization::kRootUnrolled));
  pds::Serialization serialization{objectSerializationUsed};

  if (compression == "None") {
    compression_ = pds::Compression::kNone;
  }else if (compression == "LZ4") {
    compression_ = pds::Compression::kLZ4;
  }else if (compression == "ZSTD") {
    compression_ = pds::Compression::kZSTD;
  } else {
    std::cout <<"Unknown compression algorithm '"<<compression<<"'"<<std::endl;
    throw std::runtime_error("unknown compression algorithm");
  }

  std::vector<pds::ProductInfo> productInfo;
  productInfo.reserve(typeAndNames.size());
  { 
    unsigned int index = 0;
    for( auto const& [type, name]: typeAndNames) {
      productInfo.emplace_back(name, index++, type);
    }
  }

  laneInfos_.reserve(iNLanes);
  for(unsigned int i = 0; i< iNLanes; ++i) {
    DeserializeStrategy strategy;
    switch(serialization) {
    case pds::Serialization::kRoot: { 
      strategy = DeserializeStrategy::make<DeserializeProxy<Deserializer>>(); break;
    }
    case pds::Serialization::kRootUnrolled: {
      strategy = DeserializeStrategy::make<DeserializeProxy<UnrolledDeserializer>>(); break;
    }
    }
    laneInfos_.emplace_back(productInfo, std::move(strategy));
  }


}

SharedRootBatchEventsSource::LaneInfo::LaneInfo(std::vector<pds::ProductInfo> const& productInfo, DeserializeStrategy deserialize):
  deserializers_{std::move(deserialize)},
  decompressTime_{std::chrono::microseconds::zero()},
  deserializeTime_{std::chrono::microseconds::zero()}
{
  dataProducts_.reserve(productInfo.size());
  dataBuffers_.resize(productInfo.size(), nullptr);
  deserializers_.reserve(productInfo.size());
  size_t index =0;
  for(auto const& pi : productInfo) {
    
    TClass* cls = TClass::GetClass(pi.className().c_str());
    dataBuffers_[index] = cls->New();
    assert(cls);
    dataProducts_.emplace_back(index,
			       &dataBuffers_[index],
                               pi.name(),
                               cls,
			       &delayedRetriever_);
    deserializers_.emplace_back(cls);
    ++index;
  }
}

SharedRootBatchEventsSource::LaneInfo::~LaneInfo() {
  auto it = dataProducts_.begin();
  for( void * b: dataBuffers_) {
    it->classType()->Destructor(b);
    ++it;
  }
}

size_t SharedRootBatchEventsSource::numberOfDataProducts() const {
  return laneInfos_[0].dataProducts_.size();
}

std::vector<DataProductRetriever>& SharedRootBatchEventsSource::dataProducts(unsigned int iLane, long iEventIndex) {
  return laneInfos_[iLane].dataProducts_;
}

EventIdentifier SharedRootBatchEventsSource::eventIdentifier(unsigned int iLane, long iEventIndex) {
  return laneInfos_[iLane].eventID_;
}

void SharedRootBatchEventsSource::readEventAsync(unsigned int iLane, long iEventIndex,  OptionalTaskHolder iTask) {
  //NOTE: if need future scaling performance, could move decompression out of the queue
  // and then have multiple buffers for data read from ROOT.
  queue_.push(*iTask.group(), [iLane, optTask = std::move(iTask), this]() mutable {
      auto start = std::chrono::high_resolution_clock::now();
      if(nextEntry_ < eventsTree_->GetEntries() or (cachedEventIndex_ < eventIDs_.size())) {
        if(cachedEventIndex_ == eventIDs_.size()) {
          //need to read ahead
          eventsTree_->GetEntry(nextEntry_++);

          auto start = std::chrono::high_resolution_clock::now();
          //determine uncompressed size
          const auto entriesInOffset = laneInfos_[iLane].dataProducts_.size()+1;
          unsigned int summedSizes=0;
          for(int index = 0; index < eventIDs_.size(); ++index) {
            //the last entry in the offsets is the uncompressed size for that event
            summedSizes += offsetsAndBuffer_.first[(index+1)*entriesInOffset-1];
          }
          uncompressedBuffer_ = pds::uncompressBuffer(this->compression_, offsetsAndBuffer_.second, summedSizes);
          //std::cout <<"compressed buffer size "<<offsetsAndBuffer_.second.size() <<std::endl;
          //std::cout <<"uncompressed buffer size "<<uncompressedBuffer_.size() <<std::endl;
          offsetsAndBuffer_.second = std::vector<char>(); //free memory
          laneInfos_[iLane].decompressTime_ += 
            std::chrono::duration_cast<decltype(laneInfos_[iLane].decompressTime_)>(std::chrono::high_resolution_clock::now() - start);

          cachedEventIndex_ = 0;
        }
        laneInfos_[iLane].eventID_ = eventIDs_[cachedEventIndex_];
        
        const auto entriesInOffset = laneInfos_[iLane].dataProducts_.size()+1;
        const unsigned int indexIntoOffsets = cachedEventIndex_*entriesInOffset;
        std::vector<uint32_t> offsets(offsetsAndBuffer_.first.begin()+indexIntoOffsets,
                                      offsetsAndBuffer_.first.begin()+indexIntoOffsets+entriesInOffset);

        unsigned int beginOffsetInBuffer = 0;
        for(int index = 1; index <= cachedEventIndex_; ++index) {
          beginOffsetInBuffer += offsetsAndBuffer_.first[index*entriesInOffset-1];
        }
        unsigned int endOffsetInBuffer = beginOffsetInBuffer + offsets.back();

        std::vector<char> uBuffer(uncompressedBuffer_.begin()+beginOffsetInBuffer,
                                  uncompressedBuffer_.begin()+endOffsetInBuffer);

        ++cachedEventIndex_;
        /*{
           auto const& id = this->laneInfos_[iLane].eventID_;
          std::cout <<"event entry "<<nextEntry_-1<<" cache index "<<cachedEventIndex_-1<<std::endl;
          std::cout <<"ID "<<id.run<<" "<<id.lumi<<" "<<id.event<<std::endl;
          std::cout <<"ubuffer size "<<uBuffer.size()<<std::endl;
          std::cout <<"offset size "<<offsets.size()<<std::endl;
          //for(auto b: buffer) {
          //  std::cout <<"  "<<b<<std::endl;
          //}
          }*/

        auto group = optTask.group();
        group->run([this, offsets=std::move(offsets), uBuffer = std::move(uBuffer), task = optTask.releaseToTaskHolder(), iLane]() {
            auto& laneInfo = this->laneInfos_[iLane];

            auto start = std::chrono::high_resolution_clock::now();
            //uBuffer.pop_back();
            pds::deserializeDataProducts(uBuffer.data(), uBuffer.data()+uBuffer.size(), 
                                         offsets.begin(), offsets.end(),
                                         laneInfo.dataProducts_, laneInfo.deserializers_);
            laneInfo.deserializeTime_ += 
              std::chrono::duration_cast<decltype(laneInfo.deserializeTime_)>(std::chrono::high_resolution_clock::now() - start);
          });
      }
      readTime_ +=std::chrono::duration_cast<decltype(readTime_)>(std::chrono::high_resolution_clock::now() - start);
    });
}

void SharedRootBatchEventsSource::printSummary() const {
  std::cout <<"\nSource:\n"
    "   read time: "<<readTime().count()<<"us\n"
    "   decompress time: "<<decompressTime().count()<<"us\n"
    "   deserialize time: "<<deserializeTime().count()<<"us\n"<<std::endl;
};

std::chrono::microseconds SharedRootBatchEventsSource::readTime() const {
  return readTime_;
}

std::chrono::microseconds SharedRootBatchEventsSource::decompressTime() const {
  auto time = std::chrono::microseconds::zero();
  for(auto const& l : laneInfos_) {
    time += l.decompressTime_;
  }
  return time;
}

std::chrono::microseconds SharedRootBatchEventsSource::deserializeTime() const {
  auto time = std::chrono::microseconds::zero();
  for(auto const& l : laneInfos_) {
    time += l.deserializeTime_;
  }
  return time;
}


namespace {
    class Maker : public SourceMakerBase {
  public:
    Maker(): SourceMakerBase("SharedRootBatchEventsSource") {}
      std::unique_ptr<SharedSourceBase> create(unsigned int iNLanes, unsigned long long iNEvents, ConfigurationParameters const& params) const final {
        auto fileName = params.get<std::string>("fileName");
        if(not fileName) {
          std::cout <<"no file name given\n";
          return {};
        }
        return std::make_unique<SharedRootBatchEventsSource>(iNLanes, iNEvents, *fileName);
    }
    };

  Maker s_maker;
}
