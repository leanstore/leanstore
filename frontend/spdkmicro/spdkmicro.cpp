// -------------------------------------------------------------------------------------
#include "../iob/Env.hpp"
#include "../shared-headers/Time.hpp"

#include "PerfEvent.hpp"
#include "leanstore/io/impl/Spdk.hpp"
#include "leanstore/io/IoInterface.hpp"
#include "../iob/RequestGenerator.hpp"
#include "spdk/nvme_spec.h"

#include <immintrin.h>

#include <chrono>
#include <stdlib.h>
#include <cstdint>
#include <iomanip>
#include <mutex>
#include <random>
#include <thread>
#include <unistd.h>

PerfEvent e;


/* Macros that define a rank-balanced tree */
#define RB_HEAD(name, type)                  \
struct name {                       \
   struct type *rbh_root; /* root of the tree */         \
}
#define NVME_MAX_ASYNC_EVENTS (8)

  struct nvme_async_event_request {
     struct spdk_nvme_ctrlr     *ctrlr;
     struct nvme_request     *req;
     struct spdk_nvme_cpl    cpl;
  };

  enum nvme_bp_write_state {
     SPDK_NVME_BP_WS_DOWNLOADING   = 0x0,
     SPDK_NVME_BP_WS_DOWNLOADED = 0x1,
     SPDK_NVME_BP_WS_REPLACE    = 0x2,
     SPDK_NVME_BP_WS_ACTIVATE   = 0x3,
  };


struct spdk_nvme_ctrlr {
   /* Hot data (accessed in I/O path) starts here. */

   /* Tree of namespaces */
   RB_HEAD(nvme_ns_tree, spdk_nvme_ns) ns;

   /* The number of active namespaces */
   uint32_t       active_ns_count;

   bool           is_removed;

   bool           is_resetting;

   bool           is_failed;

   bool           is_destructed;

   bool           timeout_enabled;

   /* The application is preparing to reset the controller.  Transports
    * can use this to skip unnecessary parts of the qpair deletion process
    * for example, like the DELETE_SQ/CQ commands.
    */
   bool           prepare_for_reset;

   bool           is_disconnecting;

   uint16_t       max_sges;

   uint16_t       cntlid;

   /** Controller support flags */
   uint64_t       flags;

   /** NVMEoF in-capsule data size in bytes */
   uint32_t       ioccsz_bytes;

   /** NVMEoF in-capsule data offset in 16 byte units */
   uint16_t       icdoff;

   /* Cold data (not accessed in normal I/O path) is after this point. */

   struct spdk_nvme_transport_id trid;

   union spdk_nvme_cap_register  cap;
   union spdk_nvme_vs_register   vs;

   int            state;
   uint64_t       state_timeout_tsc;

   uint64_t       next_keep_alive_tick;
   uint64_t       keep_alive_interval_ticks;

   TAILQ_ENTRY(spdk_nvme_ctrlr)  tailq;

   /** All the log pages supported */
   bool           log_page_supported[256];

   /** All the features supported */
   bool           feature_supported[256];

   /** maximum i/o size in bytes */
   uint32_t       max_xfer_size;

   /** minimum page size supported by this controller in bytes */
   uint32_t       min_page_size;

   /** selected memory page size for this controller in bytes */
   uint32_t       page_size;

   uint32_t       num_aers;
   struct nvme_async_event_request  aer[NVME_MAX_ASYNC_EVENTS];

   /** guards access to the controller itself, including admin queues */
   pthread_mutex_t         ctrlr_lock;

   struct spdk_nvme_qpair     *adminq;

   /** shadow doorbell buffer */
   uint32_t       *shadow_doorbell;
   /** eventidx buffer */
   uint32_t       *eventidx;

   /**
    * Identify Controller data.
    */
   struct spdk_nvme_ctrlr_data   cdata;

   /**
    * Zoned Namespace Command Set Specific Identify Controller data.
    */
   struct spdk_nvme_zns_ctrlr_data  *cdata_zns;

   struct spdk_bit_array      *free_io_qids;
   TAILQ_HEAD(, spdk_nvme_qpair) active_io_qpairs;

   struct spdk_nvme_ctrlr_opts   opts;

   uint64_t       quirks;

   /* Extra sleep time during controller initialization */
   uint64_t       sleep_timeout_tsc;

   /** Track all the processes manage this controller */
   TAILQ_HEAD(, spdk_nvme_ctrlr_process)  active_procs;


   STAILQ_HEAD(, nvme_request)   queued_aborts;
   uint32_t       outstanding_aborts;

   /* CB to notify the user when the ctrlr is removed/failed. */
   spdk_nvme_remove_cb        remove_cb;
   void              *cb_ctx;

   struct spdk_nvme_qpair     *external_io_msgs_qpair;
   pthread_mutex_t         external_io_msgs_lock;
   struct spdk_ring     *external_io_msgs;

   STAILQ_HEAD(, nvme_io_msg_producer) io_producers;

   struct spdk_nvme_ana_page     *ana_log_page;
   struct spdk_nvme_ana_group_descriptor  *copied_ana_desc;
   uint32_t          ana_log_page_size;

   /* scratchpad pointer that can be used to send data between two NVME_CTRLR_STATEs */
   void           *tmp_ptr;

   /* maximum zone append size in bytes */
   uint32_t       max_zone_append_size;

   /* PMR size in bytes */
   uint64_t       pmr_size;

   /* Boot Partition Info */
   enum nvme_bp_write_state   bp_ws;
   uint32_t       bpid;
   spdk_nvme_cmd_cb     bp_write_cb_fn;
   void           *bp_write_cb_arg;

   /* Firmware Download */
   void           *fw_payload;
   unsigned int         fw_size_remaining;
   unsigned int         fw_offset;
   unsigned int         fw_transfer_size;

   /* Completed register operations */
   STAILQ_HEAD(, nvme_register_completion)   register_operations;

   union spdk_nvme_cc_register      process_init_cc;
};


  struct spdk_nvme_qpair {
     struct spdk_nvme_ctrlr        *ctrlr;

     uint16_t          id;

     uint8_t              qprio;

     uint8_t              state : 3;

     uint8_t              async: 1;

     uint8_t              is_new_qpair: 1;

     /*
      * Members for handling IO qpair deletion inside of a completion context.
      * These are specifically defined as single bits, so that they do not
      *  push this data structure out to another cacheline.
      */
     uint8_t              in_completion_context : 1;
     uint8_t              delete_after_completion_context: 1;

     /*
      * Set when no deletion notification is needed. For example, the process
      * which allocated this qpair exited unexpectedly.
      */
     uint8_t              no_deletion_notification_needed: 1;

     uint8_t              last_fuse: 2;

     uint8_t              transport_failure_reason: 2;
     uint8_t              last_transport_failure_reason: 2;

     enum spdk_nvme_transport_type    trtype;

     /* request object used only for this qpair's FABRICS/CONNECT command (if needed) */
     struct nvme_request        *reserved_req;

     STAILQ_HEAD(, nvme_request)      free_req;
     STAILQ_HEAD(, nvme_request)      queued_req;

     /* List entry for spdk_nvme_transport_poll_group::qpairs */
     STAILQ_ENTRY(spdk_nvme_qpair)    poll_group_stailq;

     /** Commands opcode in this list will return error */
     TAILQ_HEAD(, nvme_error_cmd)     err_cmd_head;
     /** Requests in this list will return error */
     STAILQ_HEAD(, nvme_request)      err_req_head;

     struct spdk_nvme_ctrlr_process      *active_proc;

     struct spdk_nvme_transport_poll_group  *poll_group;

     void              *poll_group_tailq_head;

     const struct spdk_nvme_transport *transport;

     /* Entries below here are not touched in the main I/O path. */

     struct nvme_completion_poll_status  *poll_status;

     /* List entry for spdk_nvme_ctrlr::active_io_qpairs */
     TAILQ_ENTRY(spdk_nvme_qpair)     tailq;

     /* List entry for spdk_nvme_ctrlr_process::allocated_io_qpairs */
     TAILQ_ENTRY(spdk_nvme_qpair)     per_process_tailq;

     STAILQ_HEAD(, nvme_request)      aborting_queued_req;

     void              *req_buf;
  };


  /* PCIe transport extensions for spdk_nvme_qpair */
  struct nvme_pcie_qpair {
     /* Submission queue tail doorbell */
     volatile uint32_t *sq_tdbl;

     /* Completion queue head doorbell */
     volatile uint32_t *cq_hdbl;

     /* Submission queue */
     struct spdk_nvme_cmd *cmd;

     /* Completion queue */
     struct spdk_nvme_cpl *cpl;

     TAILQ_HEAD(, nvme_tracker) free_tr;
     TAILQ_HEAD(nvme_outstanding_tr_head, nvme_tracker) outstanding_tr;

     /* Array of trackers indexed by command ID. */
     struct nvme_tracker *tr;

     struct spdk_nvme_pcie_stat *stat;

     uint16_t num_entries;

     uint8_t pcie_state;

     uint8_t retry_count;

     uint16_t max_completions_cap;

     uint16_t last_sq_tail;
     uint16_t sq_tail;
     uint16_t cq_head;
     uint16_t sq_head;

     struct {
        uint8_t phase        : 1;
        uint8_t delay_cmd_submit   : 1;
        uint8_t has_shadow_doorbell   : 1;
        uint8_t has_pending_vtophys_failures : 1;
        uint8_t defer_destruction  : 1;
     } flags;

     /*
      * Base qpair structure.
      * This is located after the hot data in this structure so that the important parts of
      * nvme_pcie_qpair are in the same cache line.
      */
     struct spdk_nvme_qpair qpair;

     struct {
        /* Submission queue shadow tail doorbell */
        volatile uint32_t *sq_tdbl;

        /* Completion queue shadow head doorbell */
        volatile uint32_t *cq_hdbl;

        /* Submission queue event index */
        volatile uint32_t *sq_eventidx;

        /* Completion queue event index */
        volatile uint32_t *cq_eventidx;
     } shadow_doorbell;

     /*
      * Fields below this point should not be touched on the normal I/O path.
      */

     bool sq_in_cmb;
     bool shared_stats;

     uint64_t cmd_bus_addr;
     uint64_t cpl_bus_addr;

     struct spdk_nvme_cmd *sq_vaddr;
     struct spdk_nvme_cpl *cq_vaddr;
  };

struct nvme_pcie_ctrlr {
   struct spdk_nvme_ctrlr ctrlr;

   /** NVMe MMIO register space */
   volatile struct spdk_nvme_registers *regs;

   /** NVMe MMIO register size */
   uint64_t regs_size;

   struct {
      /* BAR mapping address which contains controller memory buffer */
      void *bar_va;

      /* BAR physical address which contains controller memory buffer */
      uint64_t bar_pa;

      /* Controller memory buffer size in Bytes */
      uint64_t size;

      /* Current offset of controller memory buffer, relative to start of BAR virt addr */
      uint64_t current_offset;

      void *mem_register_addr;
      size_t mem_register_size;
   } cmb;

   struct {
      /* BAR mapping address which contains persistent memory region */
      void *bar_va;

      /* BAR physical address which contains persistent memory region */
      uint64_t bar_pa;

      /* Persistent memory region size in Bytes */
      uint64_t size;

      void *mem_register_addr;
      size_t mem_register_size;
   } pmr;

   /** stride in uint32_t units between doorbell registers (1 = 4 bytes, 2 = 8 bytes, ...) */
   uint32_t doorbell_stride_u32;

   /* Opaque handle to associated PCI device. */
   struct spdk_pci_device *devhandle;

   /* Flag to indicate the MMIO register has been remapped */
   bool is_remapped;

   volatile uint32_t *doorbell_base;
};


struct ExtendedSpdkReq : SpdkIoReq {
   int id;
   std::vector<int>* stack;
   int* outstanding;
   uint64_t* sum;
};

int main(int , char** ) {
   std::string filename = getEnvRequired("FILENAME");
   
   int pageSize = 4*1024;
   const int iodepth = 1;
   const uint64_t iops = 1*1e6;
   const uint64_t fileSize = 100*1024*1024*1024ll;
   
   //////////////////////////////////////////////////////////////////////////////////////////////////
   ///*
   std::thread([]{
      SpdkEnvironment::init();
   }).join();
   std::unique_ptr<NVMeController> nvme = std::make_unique<NVMeController>();
   nvme->connect(filename);
   //spdk_nvme_qprio qp = SPDK_NVME_QPRIO_MEDIUM;
   //nvme->allocateQPairs(2, qp);
   nvme->allocateQPairs();
   int queue = 0;
   //auto io = [pageSize, iodepth, iops, fileSize, &nvme](int queue) {

      const auto nsSize = nvme->nsSize();
      const auto lbaSize = nvme->nsLbaDataSize();
      //char* buf = (char*)SpdkEnvironment::dma_malloc(pageSize*iodepth, 512);
      //memset(buf, 'C', pageSize*iodepth);
      std::vector<ExtendedSpdkReq> reqs(iodepth);
      std::vector<int> stack(iodepth);
      auto& nvmeRef = *nvme.get();

      uint64_t sum = 0;
      int outstanding = 0;
      const auto cal = [](SpdkIoReq* req) {
         auto ereq = reinterpret_cast<ExtendedSpdkReq*>(req);
         (*ereq->outstanding)--;
         (*ereq->stack)[*ereq->outstanding] = ereq->id;
         //sum += req->buf[0];
      };
      for (int i = 0; i < iodepth; i++) {
         auto& r = reqs[i];
         r.id = i;
         r.outstanding = &outstanding;
         r.stack = &stack;
         r.sum = &sum;
         r.type = SpdkIoReqType::Read;
         r.lba_count = pageSize/lbaSize;
         r.buf = (char*)SpdkEnvironment::dma_malloc(pageSize, 512);//buf + (i*pageSize);
         //r.buf[0] = 0;
         r.callback = cal; 
         stack[i] = i;
      }

      std::random_device rd;
      std::mt19937_64 mersene{rd()};
      std::uniform_int_distribution<uint64_t> dist(0, fileSize/pageSize);
      std::cout << "queue: " << queue << " mersen: " << mersene() << std::endl;

      uint64_t done  = 0;
      uint64_t submitted  = 0;
      uint64_t cycles = 0;
      uint64_t zero = 0;
      uint64_t timeInSubmit = 0;
      uint64_t submitCount  = 0;
      uint64_t timeInProcess = 0;
      uint64_t timeInProcessReturned0 = 0;
      uint64_t timeInProcessReturned0Count= 0;
      uint64_t timeInProcessReturnedNon0 = 0;
      uint64_t timeInProcessReturnedNon0Count = 0;
      uint64_t processCount = 0;

      std::cout << "start" << std::endl;
      const auto start = std::chrono::high_resolution_clock::now();
      const auto rs = mean::readTSC();
      auto lap = std::chrono::high_resolution_clock::now();

      uint64_t good = 0;
      uint64_t bad = 0;
      

      while (true &&  ( done < iops || outstanding > 0)) {
         //auto now = std::chrono::high_resolution_clock::now();
         //if (std::chrono::duration_cast<std::chrono::nanoseconds>(now - lap).count() > 1e9) {
         //   lap = std::chrono::high_resolution_clock::now();
         //   auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(lap - start).count();
         //   std::cout << "iops: " << done/((float)time/1e9) << " " << done*pageSize/((float)time/1e9)/1024/1024 << " MiB/s " << " io: " << done <<  " t: " << time << " s: " << submitted << " o: " << outstanding << std::endl;
         //}
         while (outstanding < iodepth && submitted < iops) {
            SpdkIoReq* r = &reqs[stack[outstanding]];
            const auto rand = dist(mersene);
            r->lba = rand*pageSize/lbaSize;
            r->buf[0] = 0;
            //std::cout << "id: " << r->id << " lba: " << r->lba << " lba_count: " << r->lba_count << " buf: " << (uint64_t) r->buf << std::endl;
   
            //if (rand % 2 == 0)
               //nvmeRef.submit(1, r);
               //nvmeRef.submit(rand % 2, r);
            //else 
            const auto submit = mean::readTSCfenced();
            nvmeRef.submit(0, r);
            const auto submitEnd = mean::readTSCfenced();
            timeInSubmit += submitEnd - submit;
            outstanding++;
            submitted++;
            submitCount++;
         }
         int d = 0;
         int nothing = 0;
         //usleep(1);
         do {

            const auto process = mean::readTSCfenced();
            //const auto p = nvmeRef.process(0, 0);
            auto qpair = nvme->qpairs[0];
            #define SPDK_CONTAINEROF(ptr, type, member) ((type *)((uintptr_t)ptr - offsetof(type, member)))
            struct nvme_pcie_qpair  *pqpair = SPDK_CONTAINEROF(qpair, nvme_pcie_qpair, qpair);
            struct spdk_nvme_cpl *cpl;
            cpl = &pqpair->cpl[pqpair->cq_head];
            int p = 0;
            //sleep(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            //raise(SIGINT);
            if (cpl->status.p == pqpair->flags.phase) {
               p = spdk_nvme_qpair_process_completions(nvme->qpairs[0], 0);
               good++;
            } else {
               p = spdk_nvme_qpair_process_completions(nvme->qpairs[0], 0);
               if (p != 0) {
                  //std::cout << "not p " << std::endl;
                  raise(SIGINT);
                  bad++;
               }
            }
            const auto processEnd = mean::readTSCfenced();

            d += p;
            timeInProcess += processEnd - process;
            if (p == 0) {
               zero++;
               timeInProcessReturned0 += processEnd - process;
               timeInProcessReturned0Count++;
            } else {
               timeInProcessReturnedNon0 += processEnd - process;
               timeInProcessReturnedNon0Count++;
            }
            processCount++;
         } while (d < 1 && submitted < iops);
         done += d;
         std::cout << done<< std::endl;
         cycles++;
      }

      std::cout << "good: " << good << " bad: " << bad << std::endl;


      /*
      uint64_t d = 0;
      {
         const uint64_t rep = 4e7;
         PerfEventBlock b{e, rep};
         for (uint64_t i = 0; i < rep; i++) {
            //const auto process = mean::readTSCfenced();
            //const int p = nvme->process(0, 0);
            const int p = spdk_nvme_qpair_process_completions(nvme->qpairs[0], 0);
         }
      }
      timeInProcessReturnedNon0Count = 1;
      //*/
      auto re = mean::readTSC();
      auto end = std::chrono::high_resolution_clock::now();
      auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      auto totalTSC = re - rs;
      float tscPerNs = (float)totalTSC / time;
      std::cout << "iops: " << iops/((float)time/1e9) << " , " << iops*pageSize/((float)time/1e9)/1024/1024 << " MiB/s " << " io: " << iops <<  " t: " << time/1e9 << "s" << std::endl;
      std::cout << "cycles: " << cycles << " zero: " << zero << std::endl;
      std::cout << "totTSC: " << totalTSC << " timeInSubmit: " << timeInSubmit << " timeInProcess: " << timeInProcess << " processCount: " << processCount << std::endl;
      std::cout << "tsc/call: " << " submit: " << timeInSubmit/submitCount<< ", " << timeInSubmit/submitCount/tscPerNs << "ns, process: " << timeInProcess/processCount  << ", " << timeInProcess/processCount/tscPerNs << "ns" << std::endl;
      std::cout << "tsc/call: " << " poll0: " << timeInProcessReturned0/timeInProcessReturned0Count << ", " << timeInProcessReturned0/timeInProcessReturned0Count/tscPerNs << "ns, count: " << timeInProcessReturned0Count/1e6 << "M";
      std::cout << ", process: " << " pollNon0: " << timeInProcessReturnedNon0/timeInProcessReturnedNon0Count << ", " << timeInProcessReturnedNon0/timeInProcessReturnedNon0Count/tscPerNs << "ns, count: " << timeInProcessReturnedNon0Count/1e6 << "M"<< std::endl;;
      std::cout << sum << std::endl;
      //SpdkEnvironment::dma_free(buf);
   //};
   //std::thread t(io, 0);
   //std::thread t2(io, 1);
   //std::thread t3(io, 2);
   //t.join();
   //t2.join();
   //t3.join();
   nvme->printPciQpairStats();
   nvme.reset(nullptr);
   SpdkEnvironment::deinit();
   //*/
   ////////////////////////////////////////////////////////////////////////////////////////
   /*
   using namespace mean;
   mean::IoOptions ioOptions("spdk", filename);
   ioOptions.iodepth = iodepth; 
   mean::IoInterface::initInstance(ioOptions);

   auto io = [&](int queue) {
      //char* buf = (char*)SpdkEnvironment::dma_malloc(pageSize*iodepth, 512);
      //memset(buf, 'C', pageSize*iodepth);
      auto& ioChannel = mean::IoInterface::instance().getIoChannel(queue);
      std::vector<IoBaseRequest> reqs(iodepth);
      std::vector<int> stack(iodepth);

      uint64_t sum = 0;
      int outstanding = 0;
      auto cal =  [&stack, &outstanding, &sum](const IoBaseRequest* req, uintptr_t, uintptr_t) {
         outstanding--;
         stack[outstanding] = req->id;
         sum += req->data[0];
      };
      for (int i = 0; i < iodepth; i++) {
         auto& r = reqs[i];
         r.id = i;
         r.type = IoRequestType::Read;
         r.len = pageSize;
         r.data = (char*)IoInterface::allocIoMemoryChecked(pageSize, 512);//buf + (i*pageSize);
         r.data[0] = 0;
         r.callback = cal; 
         stack[i] = i;
      }

      std::random_device rd;
      std::mt19937_64 mersene{rd()};
      std::cout << "m: " << mersene() << std::endl;
      std::uniform_int_distribution<uint64_t> dist(0, fileSize/pageSize);

      uint64_t done  = 0;
      uint64_t submitted  = 0;

      std::cout << "start" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();
      auto rs = mean::readTSC();
      auto lap = std::chrono::high_resolution_clock::now();
      while (done < iops || outstanding > 0) {
         auto now = std::chrono::high_resolution_clock::now();
         if (std::chrono::duration_cast<std::chrono::nanoseconds>(now - lap).count() > 1e9) {
            lap = std::chrono::high_resolution_clock::now();
            auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(lap - start).count();
            std::cout << "iops: " << done/((float)time/1e9) << " " << done*pageSize/((float)time/1e9)/1024/1024 << " MiB/s " << " io: " << done <<  " t: " << time << " s: " << submitted << " o: " << outstanding << std::endl;
         }
         
         auto rr = mean::readTSC();
         while (outstanding < iodepth && submitted < iops) {
            IoBaseRequest& r = reqs[stack[outstanding]];
            r.addr = dist(mersene)*pageSize;
            r.data[0] = 0;
            //std::cout << "id: " << r->id << " lba: " << r->lba << " lba_count: " << r->lba_count << " buf: " << (uint64_t) r->buf << std::endl;
            ioChannel.push(r);
            ioChannel.submit();
            outstanding++;
            submitted++;
         }
         done += ioChannel.poll();
      }
      auto re = mean::readTSC();
      auto end = std::chrono::high_resolution_clock::now();
      auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      std::cout << "iops: " << iops/((float)time/1e9) << " " << iops*pageSize/((float)time/1e9)/1024/1024 << " MiB/s " << " io: " << iops <<  " t: " << time << std::endl;

      std::cout << sum << std::endl;
   };
   std::thread t(io, 0);
   std::thread t2(io, 1);
   t.join();
   t2.join();
   //*/
   ////////////////////////////////////////////////////////////////////////////////////////
   /*
   mean::IoOptions ioOptions("spdk", filename);
   ioOptions.iodepth = iodepth; 
   mean::IoInterface::initInstance(ioOptions);

   mean::JobOptions opts;
   opts.bs = pageSize;
   opts.io_size = iops*pageSize;
   opts.iodepth = iodepth;
   opts.filesize = fileSize;
   mean::RequestGenerator gen("name", opts, mean::IoInterface::instance().getIoChannel(0), 1);

   auto start = std::chrono::high_resolution_clock::now();
   gen.runIo();
   auto end = std::chrono::high_resolution_clock::now();
   auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
   std::cout << "iops: " << iops/((float)time/1e9) << " " << iops*pageSize/((float)time/1e9)/1024/1024 << " MiB/s " << " io: " << iops <<  " t: " << time << std::endl;
   //*/
   return 0;
}
