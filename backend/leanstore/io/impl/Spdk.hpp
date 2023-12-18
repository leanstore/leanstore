#pragma once
#if LEANSTORE_INCLUDE_SPDK
// -------------------------------------------------------------------------------------
#include "../Raid.hpp"
// -------------------------------------------------------------------------------------
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_spec.h"
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <vector>
#include <thread>
#include <iomanip>
#include <iostream>
#include <cassert>
#include <sstream>
#include <mutex>
#include <cstring>
#include <stdexcept>
// -------------------------------------------------------------------------------------
#define checkThrow(test, message) do { if (!test) { throw std::logic_error(message); }} while(false);
#define checkMessage(test, message) do { if (!test) { std::cout << message << std::endl; }} while(false);
// -------------------------------------------------------------------------------------
enum class SpdkIoReqType {
	Write = 0,
   Read = 1,
   ZnsAppend = 2,
   COUNT = 3// always last 
   // Don't forget to add pointers to  spdk_req_type_fun_lookup in SpdkEnv init
};
// -------------------------------------------------------------------------------------
struct SpdkIoReq;
using SpdkIoReqCallback = void(*)(SpdkIoReq* req);
struct SpdkIoReq {
	char* buf;
	uint64_t lba;
	uint64_t append_lba; //
	SpdkIoReqCallback callback;
	spdk_nvme_qpair* qpair;
	uint32_t lba_count;
   //
   void* this_ptr;
	SpdkIoReqType type;
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using cmd_fun = int (*)(spdk_nvme_ns*, spdk_nvme_qpair*, void*, uint64_t, uint32_t, spdk_nvme_cmd_cb, void*, uint32_t);
class SpdkEnvironment {
   static bool initialized;
	static bool isInitialized(bool init = false) ;;
public:
   static cmd_fun spdk_req_type_fun_lookup[(int)SpdkIoReqType::COUNT+1];
	static void ensureInitialized(); 
	static void init(); 
	static void deinit(); 
	static void* dma_malloc(size_t size, size_t alignment = 0, uint64_t* phys_addr = nullptr) ;;
	static void dma_free(void* buf) ;;
};
// -------------------------------------------------------------------------------------
class NVMeInterface {
public:
	virtual void connect(std::string connectionString) = 0;
	virtual void allocateQPairs() = 0;
	virtual int32_t qpairSize() = 0;
	virtual uint32_t nsLbaDataSize() = 0;
	virtual uint64_t nsSize() = 0;
	virtual void submit(int queue, SpdkIoReq* req) = 0; 
	virtual int32_t process(int queue, int max) = 0;
	virtual ~NVMeInterface(){ };
};
// -------------------------------------------------------------------------------------
class NVMeController : public NVMeInterface {
public:
	std::string pciefile;
	struct spdk_nvme_transport_id trid;
	struct spdk_nvme_ctrlr *ctrlr = nullptr;
	struct spdk_nvme_ns* nameSpace;
   // -------------------------------------------------------------------------------------
   uint64_t ns_capcity_lbas = 0;
   // -------------------------------------------------------------------------------------
   uint64_t dones = 0;
   uint64_t submitted = 0;
   // Info
   const struct spdk_nvme_ctrlr_data *cdata;
   int maxQPairs = -1;
   std::vector<spdk_nvme_qpair*> qpairs;
   // the poll groups aren't really used. They're needed to access pci statistics.
   // see printPciQpairStats
   std::vector<spdk_nvme_poll_group*> pollGroups; 
   // -------------------------------------------------------------------------------------
   NVMeController();
   ~NVMeController() override; 
   // -------------------------------------------------------------------------------------
   void connect(std::string pciefile) override; 
   uint32_t nsLbaDataSize() override; 
   uint64_t nsSize() override; 
	void allocateQPairs() override; 
   // -------------------------------------------------------------------------------------
   int32_t process(int qpair, int max) {
      int ok = 0;
      if (submitted - dones > 0) {
         ok = spdk_nvme_qpair_process_completions(qpairs[qpair], max);
         dones += ok;
         assert(ok >= 0);
      }
      return ok; 
   }
   // -------------------------------------------------------------------------------------
	static void completion(void *cb_arg, const struct spdk_nvme_cpl *cpl);
   // -------------------------------------------------------------------------------------
   void submitCheck(int ret, int submitted, int dones) {
      if (ret == -ENOMEM) {
         throw std::logic_error("Could not submit io. ret: " + std::to_string(ret) + " ENOMEM (outstanding ios:  " + std::to_string(submitted - dones));
      } else {
         throw std::logic_error("Could not submit io. ret: " + std::to_string(ret) + " sub: " + std::to_string(submitted) + " done: " + std::to_string(dones));
      }
   }
   // -------------------------------------------------------------------------------------
   void submit(int qpair, SpdkIoReq* req)  { //, SpdkIoCallback callback) {
      //std::stringstream ss;
      //ss << pciefile << " qpair: "<< qpair << " submit: t: " << (int)req->type << " lba: "<< req->lba << " cnt: " << req->lba_count << std::endl;
      //std:: cout << ss.str();
      assert(req->lba_count > 0);
      //if (req->lba_count != 1 ) throw "";
      req->qpair = qpairs[qpair];
      int ret = SpdkEnvironment::spdk_req_type_fun_lookup[(int)req->type](nameSpace, qpairs[qpair], req->buf, req->lba, req->lba_count, completion, req, 0);
      /*
         int ret;
         if (req->type == SpdkIoReqType::Read) {
         ret = spdk_nvme_ns_cmd_read(nameSpace, qpairs[qpair], req->buf, req->lba, req->lba_count, completion, req, 0);
         } else {
      //std::cout << "write: cb_arg: " << std::hex << (u64)&req <<
      ret = spdk_nvme_ns_cmd_write(nameSpace, qpairs[qpair], req->buf, req->lba, req->lba_count, completion, req, 0);
      }
      ///*/
      if (ret != 0) {
         submitCheck(ret, submitted, dones);
      }
      submitted++;
   }
	uint32_t numberNamespaces(); 
	uint64_t nsNumLbas(); 
	void allocateQPairs(int number, enum spdk_nvme_qprio prio); 
	int32_t qpairSize() override; 
	int requestMaxQPairs(); 
	void requestMaxQPairs(int32_t& subQs, int32_t& compQs); 
	bool requestFeature(int32_t feature, struct spdk_nvme_cpl& cpl); 
	bool pushRawCmd(struct spdk_nvme_cmd& cmd, struct spdk_nvme_cpl& cpl); 
	bool checkArbitrationRoundRobinCap(); 
	void setArbitration(int low, int middle, int high); 
	void printArbitrationFeatures(); 
	void printPciQpairStats(); 
};
// -------------------------------------------------------------------------------------
class NVMeMultiController {
public:
	std::vector<NVMeController> controller;
	void connect(std::string connectionString) ; 
	void allocateQPairs() ; 
	int32_t qpairSize() ; 
	// OPTIMIZE
   uint32_t nsLbaDataSize() ; 
   uint64_t nsSize() ; 
   int deviceCount(); 
   // -------------------------------------------------------------------------------------
   void submit(int device, int queue, SpdkIoReq* req)  {
      controller[device].submit(queue, req);
   }
   // -------------------------------------------------------------------------------------
   int32_t process(int queue, int max)  {
      int32_t done = 0;
      for (auto& c: controller) {
         done += c.process(queue, max);
      }
      return done;
   }
};
// -------------------------------------------------------------------------------------
class NVMeRaid0 : public NVMeInterface {
	std::vector<std::unique_ptr<NVMeController>> controller;
	const uint32_t lbasPerDevice = 2048; // 512*32
public:
	void connect(std::string connectionString) override; 
   void allocateQPairs() override; 
   int32_t qpairSize() override; 
   // OPTIMIZE
   uint32_t nsLbaDataSize() override; 
   uint64_t nsSize() override; 
   void submit(int queue, SpdkIoReq* req) override; 
   int32_t process(int queue, int max) override; 
};
#endif
