#if LEANSTORE_INCLUDE_SPDK
#include "Spdk.hpp"
#include "spdk/nvme_spec.h"
#include "spdk/nvme_zns.h"
#include <cstddef>
#include <regex>

bool SpdkEnvironment::initialized = false;
cmd_fun SpdkEnvironment::spdk_req_type_fun_lookup[(int)SpdkIoReqType::COUNT+1];
bool SpdkEnvironment::isInitialized(bool init) {
   if (init) {
      initialized = init;
   }
   return initialized;
};
void SpdkEnvironment::ensureInitialized() {
   if (!isInitialized()) {
      throw std::logic_error("SpdkEnvironment not initialized");
   }
}
void SpdkEnvironment::init() {
   if (isInitialized()) {
      throw std::logic_error("SpdkEnvironment already initialized");
   }
   SpdkEnvironment::spdk_req_type_fun_lookup[(int)SpdkIoReqType::Read] = &spdk_nvme_ns_cmd_read;
   SpdkEnvironment::spdk_req_type_fun_lookup[(int)SpdkIoReqType::Write] = &spdk_nvme_ns_cmd_write;
   SpdkEnvironment::spdk_req_type_fun_lookup[(int)SpdkIoReqType::ZnsAppend] = &spdk_nvme_zns_zone_append;
   SpdkEnvironment::spdk_req_type_fun_lookup[(int)SpdkIoReqType::COUNT] = nullptr;

   struct spdk_env_opts opts;
   int g_shm_id = -1;
   int g_dpdk_mem = 0;
   int g_main_core = 0; // master must be in core_mask e.g. 0 in 0x1
   char g_core_mask[16] = "0x1"; //"0xffffffff";
   //bool g_vmd = false;

   spdk_env_opts_init(&opts);
   opts.name = "meanio_spdk_env_name";
   opts.shm_id = g_shm_id;
   opts.mem_size = g_dpdk_mem;
   opts.mem_channel = 1;
   opts.main_core = g_main_core;
   opts.core_mask = g_core_mask;
   /*
      opts.iova_mode = "va";
      opts.env_context = (void*)"--log-level=lib.eal:8";
      */

   if (spdk_env_init(&opts) < 0) {
      throw std::logic_error("Unable to initialize SPDK env. \n 1) Are you sure you are running with sufficien privileges? \n 2) Is the SPDK environment setup(.sh)?");
   }
   /*
      if (g_vmd && spdk_vmd_init()) {
      throw std::logic_error("Failed to initialize VMD. Some NVMe devices can be unavailable.");
      }
      */
   isInitialized(true);
}
void SpdkEnvironment::deinit() {
   if (isInitialized()) {
      spdk_env_fini();
   }
}

void* SpdkEnvironment::dma_malloc(size_t size, size_t alignment, uint64_t* phys_addr) {
   ensureInitialized();
   return spdk_dma_malloc(size, alignment, phys_addr);
};

void SpdkEnvironment::dma_free(void* buf) {
   ensureInitialized();
   return spdk_dma_free(buf);
};
// -------------------------------------------------------------------------------------
// NVMeController
// -------------------------------------------------------------------------------------
NVMeController::NVMeController() {
   SpdkEnvironment::ensureInitialized();
}
// -------------------------------------------------------------------------------------
NVMeController::~NVMeController() {
   if (ctrlr) {
      std::cout << "NVMeController destructor submitted: " << submitted << " dones: " << dones << std::endl;
      //printPciQpairStats();
      for (auto q: qpairs) {
         spdk_nvme_ctrlr_free_io_qpair(q);
      }
      spdk_nvme_detach(ctrlr);
   }
}
// -------------------------------------------------------------------------------------
void NVMeController::connect(std::string pciefile)  {
   this->pciefile = pciefile;
   std::memset(&trid, 0, sizeof(trid));
   spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
   snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);

   if (spdk_nvme_transport_id_parse(&trid, pciefile.c_str()) != 0) {
      throw std::logic_error("Error parsing transport address.");
   }
   spdk_nvme_transport_id_populate_trstring(&trid, spdk_nvme_transport_id_trtype_str(trid.trtype));
   ctrlr = spdk_nvme_connect(&trid, NULL, 0);
   if (!ctrlr) {
      throw std::logic_error("spdk_nvme_connect() failed");
   }
   cdata = spdk_nvme_ctrlr_get_data(ctrlr);

   std::regex rns ("ns=(\\d+)");
   std::smatch nsm;
   std::regex_search(pciefile, nsm, rns);
   int ns = 1;
   if (nsm.size() > 1) {
      std::cout << "nsm: " << nsm[1] << std::endl;
      ns = stoi(nsm[1]);
   }

   nameSpace = spdk_nvme_ctrlr_get_ns(ctrlr, ns);
   assert(nameSpace);
   if (!spdk_nvme_ns_is_active(nameSpace)) {
      throw std::logic_error("namespace not active. spdk_nvme_ns_is_active");
   }

   char tmpStr[1024];
   snprintf(tmpStr, sizeof(tmpStr), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);
   std::string name(tmpStr);
   std::cout << "Connected: " << trid.traddr << " name: " << name;
   std::cout << " size: " << nsSize()/1024/1024/1024 << " GiB (" << nsSize() << " byte)";
   std::cout << " sector size: " << spdk_nvme_ns_get_sector_size(nameSpace);
   std::cout << " namespaces: " << numberNamespaces();
   std::cout << " max_io_xfer_size: " << spdk_nvme_ns_get_max_io_xfer_size(nameSpace) << std::endl;


   const struct spdk_nvme_ns_data*  ns_data = spdk_nvme_ns_get_data(spdk_nvme_ctrlr_get_ns(ctrlr, ns));
   std::cout << "selected ns: " << ns << " active: " << spdk_nvme_ns_is_active(nameSpace) << " nsze: " << ns_data->nsze << " ncap: " << ns_data->ncap << " nuse: "<< ns_data->nuse << std::endl;
	ns_capcity_lbas = ns_data->ncap;

	/*
   for (unsigned int i = 1; i < numberNamespaces() + 1; i++) {
      const struct spdk_nvme_ns_data*  ns_data = spdk_nvme_ns_get_data(spdk_nvme_ctrlr_get_ns(ctrlr, i));
      std::cout << "ns: " << i << " active: " << spdk_nvme_ns_is_active(spdk_nvme_ctrlr_get_ns(ctrlr, i)) << " nsze: " << ns_data->nsze << " ncap: " << ns_data->ncap << " nuse: "<< ns_data->nuse << std::endl;
   }
	*/
}
// -------------------------------------------------------------------------------------
uint32_t NVMeController::nsLbaDataSize()  {
   return spdk_nvme_ns_get_sector_size(nameSpace); 
}
// -------------------------------------------------------------------------------------
uint64_t NVMeController::nsSize()  {
   return spdk_nvme_ns_get_size(nameSpace);
}
// -------------------------------------------------------------------------------------
void NVMeController::allocateQPairs()  {
   allocateQPairs(-1, SPDK_NVME_QPRIO_MEDIUM);
}
// -------------------------------------------------------------------------------------
void NVMeController::completion(void *cb_arg, const struct spdk_nvme_cpl *cpl) {
   auto request = static_cast<SpdkIoReq*>(cb_arg);
   if (spdk_nvme_cpl_is_error(cpl)) {
      spdk_nvme_qpair_print_completion(request->qpair, (struct spdk_nvme_cpl *)cpl);
      throw std::logic_error("spdk completion failed: " 
            + std::string(spdk_nvme_cpl_get_status_string(&cpl->status)) 
            + "\nnote this often happens when buffers have not been allocated using spdk rte.");
   }
	request->append_lba = cpl->cdw0; // new lba set by zone_append
   //std::cout << "completion: req: "<< std::hex << (uint64_t)request << " id: " << request->id << " buf: " << (uint64_t) request->buf << std::endl;
   request->callback(request);
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
uint32_t NVMeController::numberNamespaces() {
   return spdk_nvme_ctrlr_get_num_ns(ctrlr);
}
// -------------------------------------------------------------------------------------
uint64_t NVMeController::nsNumLbas() {
   return spdk_nvme_ns_get_num_sectors(nameSpace);
}
// -------------------------------------------------------------------------------------
void NVMeController::allocateQPairs(int number, enum spdk_nvme_qprio /*prio*/) {
   if (number < 0) {
      number = requestMaxQPairs();
   }
   for (int i = 0; i < number; i++) {
      struct spdk_nvme_io_qpair_opts opts;
      spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
      //opts.qprio = static_cast<spdk_nvme_qprio>(prio & SPDK_NVME_CREATE_IO_SQ_QPRIO_MASK);
      opts.delay_cmd_submit = true;
      opts.create_only = true;
      //std::cout << "doorbell: " << opts.delay_cmd_submit << " opts.io_queue_size: " << opts.io_queue_size << std::endl;
      auto* qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));
      if (!qpair) {
         throw std::logic_error("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
      }
      qpairs.push_back(qpair);
      auto* group = spdk_nvme_poll_group_create(NULL, NULL);
      int r = spdk_nvme_poll_group_add(group, qpair);
      if (r) {
         throw std::logic_error("can't add to poll group err: " + std::to_string(r));
      }
      r = spdk_nvme_ctrlr_connect_io_qpair(ctrlr, qpair);
      if (r) {
         throw std::logic_error("can't connect qpair err: " + std::to_string(r));
      }
      pollGroups.push_back(group);
      //cout << "qpair attached: " << i << endl;
   }
}
// -------------------------------------------------------------------------------------
int32_t NVMeController::qpairSize()  {
   return qpairs.size();
}
// -------------------------------------------------------------------------------------
int NVMeController::requestMaxQPairs() {
   int sub, comp;
   requestMaxQPairs(sub, comp);
   assert(sub == comp);
   return sub; 
}
// -------------------------------------------------------------------------------------
void NVMeController::requestMaxQPairs(int32_t& subQs, int32_t& compQs) {
   struct spdk_nvme_cpl cpl;
   requestFeature(SPDK_NVME_FEAT_NUMBER_OF_QUEUES, cpl);
   int32_t ret = cpl.cdw0;
   subQs = (ret & 0xFFFF) + 1;
   compQs = (ret & 0xFFFF0000 >> 16) + 1;
}
// -------------------------------------------------------------------------------------
bool NVMeController::requestFeature(int32_t feature, struct spdk_nvme_cpl& cpl) {
   /*
      auto fn = [](void *myCplPtr, const struct spdk_nvme_cpl *cpl) {
      struct spdk_nvme_cpl* myCpl = reinterpret_cast<struct spdk_nvme_cpl*>(myCplPtr);
      if (spdk_nvme_cpl_is_error(cpl)) {
      std::cout << "get_feature() failed" << std::endl;
      }
    *myCpl = *cpl;
    };
    */
   struct spdk_nvme_cmd cmd = {};
   cmd.opc = SPDK_NVME_OPC_GET_FEATURES;
   cmd.cdw10_bits.get_features.fid = feature; 

   return pushRawCmd(cmd, cpl);
}
// -------------------------------------------------------------------------------------
bool NVMeController::pushRawCmd(struct spdk_nvme_cmd& cmd, struct spdk_nvme_cpl& cpl) {
   auto fn = [](void *myCplPtr, const struct spdk_nvme_cpl *cpl) {
      struct spdk_nvme_cpl* myCpl = reinterpret_cast<struct spdk_nvme_cpl*>(myCplPtr);
      if (spdk_nvme_cpl_is_error(cpl)) {
         std::cout << "get_feature() failed" << std::endl;
      }
      *myCpl = *cpl;
   };
   spdk_nvme_ctrlr_cmd_admin_raw(ctrlr, &cmd, NULL, 0, fn, &cpl);
   int outstanding_commands = 1;
   while (outstanding_commands) {
      outstanding_commands -= spdk_nvme_ctrlr_process_admin_completions(ctrlr);
   }
   bool ok = !spdk_nvme_cpl_is_error(&cpl); 
   //checkThrow(ok, "pushRawCmd failed");
   return ok; 
}
// -------------------------------------------------------------------------------------
bool NVMeController::checkArbitrationRoundRobinCap() {
   union spdk_nvme_cap_register cap = spdk_nvme_ctrlr_get_regs_cap(ctrlr);
   return cap.bits.ams & SPDK_NVME_CAP_AMS_WRR;
}
// -------------------------------------------------------------------------------------
void NVMeController::setArbitration(int low, int middle, int high) {
   struct spdk_nvme_cmd cmd = {};
   checkMessage(checkArbitrationRoundRobinCap(), "Arbitration WRR not supported");

   cmd.opc = SPDK_NVME_OPC_SET_FEATURES;
   cmd.cdw10_bits.set_features.fid = SPDK_NVME_FEAT_ARBITRATION;
   cmd.cdw11_bits.feat_arbitration.bits.ab = SPDK_NVME_ARBITRATION_BURST_UNLIMITED;
   cmd.cdw11_bits.feat_arbitration.bits.lpw = low;
   cmd.cdw11_bits.feat_arbitration.bits.mpw = middle;
   cmd.cdw11_bits.feat_arbitration.bits.hpw = high;
   struct spdk_nvme_cpl cpl = {};
   bool ok = pushRawCmd(cmd, cpl);
   checkThrow(ok, "setArbitration failed");
   //ret = spdk_nvme_ctrlr_cmd_admin_raw(ctrlr, &cmd, NULL, 0,
   //		set_feature_completion, &features[SPDK_NVME_FEAT_ARBITRATION]);
}
// -------------------------------------------------------------------------------------
void NVMeController::printArbitrationFeatures() {
   struct spdk_nvme_cpl cpl = {};
   requestFeature(SPDK_NVME_FEAT_ARBITRATION, cpl);
   union spdk_nvme_cmd_cdw11 arb;
   arb.feat_arbitration.raw = cpl.cdw0;

   printf("Current Arbitration Configuration\n");
   printf("===========\n");
   printf("Arbitration Burst:					 ");
   if (arb.feat_arbitration.bits.ab == SPDK_NVME_ARBITRATION_BURST_UNLIMITED) {
      printf("no limit\n");
   } else {
      printf("%u\n", 1u << arb.feat_arbitration.bits.ab);
   }		 

   printf("Low Priority Weight:				 %u\n", arb.feat_arbitration.bits.lpw + 1);
   printf("Medium Priority Weight:			 %u\n", arb.feat_arbitration.bits.mpw + 1);
   printf("High Priority Weight:				 %u\n", arb.feat_arbitration.bits.hpw + 1);
   printf("\n");
}
// -------------------------------------------------------------------------------------
void NVMeController::printPciQpairStats() {
   for (auto group: pollGroups) {
      struct spdk_nvme_poll_group_stat *stat = NULL;
      if(spdk_nvme_poll_group_get_stats(group, &stat)){
         throw std::logic_error("");
      }
      for (unsigned i = 0; i < stat->num_transports; i++) {
         assert(stat->transport_stat[i]->trtype);
         struct spdk_nvme_pcie_stat* pcie_stat = &stat->transport_stat[i]->pcie;
         if (pcie_stat->polls != 0) {
            std::cout << "polls: " << pcie_stat->polls;
            std::cout << " idle_polls: " << pcie_stat->idle_polls;
            std::cout << " completions: " << pcie_stat->completions;
            std::cout << " sq_mmio_doorbell_updates: " << pcie_stat->sq_mmio_doorbell_updates;
            std::cout << " cq_mmio_doorbell_updates: " << pcie_stat->cq_mmio_doorbell_updates;
            std::cout << " queued_requests: " << pcie_stat->queued_requests;
            std::cout << " submitted_requests: " << pcie_stat->submitted_requests;
            std::cout << std::endl;
         }
      }
   }
}
/*
   static void spdk_nvme_io_reset_sgl(void *ref, uint32_t sgl_offset)
   {
   struct spdk_fio_request *fio_req = (struct spdk_fio_request *)ref;
   fio_req->iov_offset = sgl_offset;
   }

   static int spdk_nvme_io_next_sge(void *ref, void **address, uint32_t *length)
   {
   struct spdk_fio_request *fio_req = (struct spdk_fio_request *)ref;
   struct io_u *io_u = fio_req->io;
   uint32_t iov_len;

 *address = io_u->buf;

 if (fio_req->iov_offset) {
 assert(fio_req->iov_offset <= io_u->xfer_buflen);
 *address += fio_req->iov_offset;
 }

 iov_len = io_u->xfer_buflen - fio_req->iov_offset;
 if (iov_len > g_spdk_sge_size) {
 iov_len = g_spdk_sge_size;
 }

 fio_req->iov_offset += iov_len;
 *length = iov_len;
 return 0;
 }

 int submitv(int qpair, SpdkIoReq& req) {
//std::cout << "submit: t: " << (int)req.type << " lba: "<< req.lba << " cnt: " << req.lba_count << std::endl;
if (req.type == SpdkIoReqType::Read) {
return spdk_nvme_ns_cmd_read(nameSpace, qpairs[qpair], req.buf, req.lba, req.lba_count, completion, &req, 0);
} else {
return spdk_nvme_ns_cmd_write(nameSpace, qpairs[qpair], req.buf, req.lba, req.lba_count, completion, &req, 0);
}
}
*/
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
void NVMeMultiController::connect(std::string connectionString)  {
   std::istringstream iss(connectionString);
   std::string device;
   std::vector<std::string> devices;
   while (std::getline(iss, device, ';')) {
      devices.push_back(device);
   }
   controller.resize(devices.size());
   for (unsigned int i = 0; i < devices.size(); i++) {
      controller[i].connect(devices[i]);
   }
}
// -------------------------------------------------------------------------------------
void NVMeMultiController::allocateQPairs()  {
   for (auto& c: controller) {
      c.allocateQPairs();
   }
}
// -------------------------------------------------------------------------------------
int32_t NVMeMultiController::qpairSize()  {
   int32_t min = std::numeric_limits<int32_t>::max();
   for (auto& c: controller) {
      min = std::min(min, c.qpairSize());
   }
   if (min <= 0 || min == std::numeric_limits<int32_t>::max()) {
      throw std::logic_error("could not get qpair size");
   }
   return min;
}
// -------------------------------------------------------------------------------------
// OPTIMIZE
uint32_t NVMeMultiController::nsLbaDataSize()  {
   uint32_t dataSize = controller[0].nsLbaDataSize();
   for (auto& c: controller) {
      if (dataSize != c.nsLbaDataSize()) {
         throw std::logic_error("lba size must be equal for all devices");
      }
   }
   return dataSize;
}
// -------------------------------------------------------------------------------------
uint64_t NVMeMultiController::nsSize()  {
   uint64_t size = std::numeric_limits<uint64_t>::max();
   for (auto& c: controller) {
      size = std::min(size, c.nsSize());
   }
   return size;
}
// -------------------------------------------------------------------------------------
int NVMeMultiController::deviceCount() {
   return controller.size();
}
// -------------------------------------------------------------------------------------
void NVMeRaid0::connect(std::string connectionString)  {
   std::istringstream iss(connectionString);
   std::string device;
   while (std::getline(iss, device, ';')) {
      controller.emplace_back(std::make_unique<NVMeController>());
      controller.back()->connect(device);
   }
}
// -------------------------------------------------------------------------------------
void NVMeRaid0::allocateQPairs()  {
   for (auto& c: controller) {
      c->allocateQPairs();
   }
}
// -------------------------------------------------------------------------------------
int32_t NVMeRaid0::qpairSize()  {
   int32_t min = std::numeric_limits<int32_t>::max();
   for (auto& c: controller) {
      min = std::min(min, c->qpairSize());
   }
   if (min <= 0 || min == std::numeric_limits<int32_t>::max()) {
      throw std::logic_error("could not get qpair size");
   }
   return min;
}
// -------------------------------------------------------------------------------------
// OPTIMIZE
uint32_t NVMeRaid0::nsLbaDataSize()  {
   uint32_t dataSize = controller[0]->nsLbaDataSize();
   for (auto& c: controller) {
      if (dataSize != c->nsLbaDataSize()) {
         throw std::logic_error("lba size must be equal for all devices");
      }
   }
   return dataSize;
}
// -------------------------------------------------------------------------------------
uint64_t NVMeRaid0::nsSize()  {
   uint64_t size = std::numeric_limits<uint64_t>::max();
   for (auto& c: controller) {
      size = std::min(size, c->nsSize());
   }
   return size;
}
// -------------------------------------------------------------------------------------
void NVMeRaid0::submit(int queue, SpdkIoReq* req)  {
   const int deviceCnt = controller.size();
   assert(lbasPerDevice >= req->lba_count);
   Raid0 raidImpl(deviceCnt, lbasPerDevice); // non-generic raid0 implementation.
   int deviceSelector;
   raidImpl.calc(req->lba, deviceSelector, req->lba);
   controller[deviceSelector]->submit(queue, req);
}
// -------------------------------------------------------------------------------------
int32_t NVMeRaid0::process(int queue, int max)  {
   int32_t done = 0;
   for (auto& c: controller) {
      done += c->process(queue, max);
   }
   return done;
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#endif
