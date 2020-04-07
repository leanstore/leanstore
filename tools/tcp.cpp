#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <arpa/inet.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(threads, 1, "");
DEFINE_uint64(port, 9090, "");
DEFINE_uint64(run_for_seconds, 0, "");
DEFINE_string(host, "127.0.0.1", "");
DEFINE_bool(client, false, "");
DEFINE_bool(server, false, "");
DEFINE_bool(profile, false, "");
DEFINE_uint64(packet_size, 15000, "1500-65535 bytes");
// -------------------------------------------------------------------------------------
struct alignas(64) CacheLine {
  CacheLine() { counter = 0; }
  std::atomic<u64> counter = 0;
};
// -------------------------------------------------------------------------------------
using namespace std;
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  vector<thread> threads;
  auto threads_cl = std::make_unique<CacheLine[]>(1024);
  atomic<u64> threads_counter = 0;
  // -------------------------------------------------------------------------------------
  if (FLAGS_server) {
    // -------------------------------------------------------------------------------------
    const u64 BUFFER_SIZE = 1024 * 1024 * 0.5;  // 0.5 MiB
    // -------------------------------------------------------------------------------------
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    // Creating socket file descriptor
    posix_check(server_fd = socket(AF_INET, SOCK_STREAM, 0));
    posix_check(setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)) == 0);

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(FLAGS_port);

    posix_check(bind(server_fd, (struct sockaddr*)&address, sizeof(address)) >= 0);
    posix_check(listen(server_fd, 300) >= 0);
    if (!FLAGS_profile)
      threads.emplace_back([&]() {
        while (true) {
          u64 total = 0;
          for (u64 t_i = 0; t_i < threads_counter; t_i++) {
            total += threads_cl[t_i].counter.exchange(0);
          }
          double gib = total / 1024.0 / 1024.0 / 1024.0;
          cout << gib << '\t' << threads_counter.load() << endl;
          sleep(1);
        }
      });
    while (true) {
      posix_check((new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen)) >= 0);
      threads.emplace_back(
          [&threads_cl](const int socket, const u64 t_i) {
            auto buffer = std::make_unique<u8[]>(BUFFER_SIZE);
            int read_length;
            if (FLAGS_profile) {
              CacheLine local_counter;
              PerfEvent e;
              PerfEventBlock b(e, 1);
              while ((read_length = read(socket, buffer.get(), BUFFER_SIZE)) > 0) {
                local_counter.counter += read_length;
              }
              double gib = local_counter.counter / 1024.0 / 1024.0 / 1024.0;
              b.scale = local_counter.counter.load();
              cout << gib << endl;
            } else {
              while ((read_length = read(socket, buffer.get(), BUFFER_SIZE)) > 0) {
                threads_cl[t_i].counter += read_length;
              }
            }
            close(socket);
          },
          new_socket, threads_counter++);
    }
    for (auto& thread : threads) {
      thread.join();
    }
  } else if (FLAGS_client) {
    auto dummy_packet = std::make_unique<u8[]>(FLAGS_packet_size);
    for (u64 c_i = 0; c_i < FLAGS_packet_size; c_i++) {
      dummy_packet[c_i] = rand() % 255;
    }
    // -------------------------------------------------------------------------------------
    atomic<bool> keep_running = true;
    atomic<s64> threads_running = 0;
    // -------------------------------------------------------------------------------------
    if (!FLAGS_profile)
      threads.emplace_back([&]() {
        threads_running++;
        u64 second = 0;
        while (keep_running) {
          u64 total = 0;
          for (u64 t_i = 0; t_i < threads_counter; t_i++) {
            total += threads_cl[t_i].counter.exchange(0);
          }
          double gib = total / 1024.0 / 1024.0 / 1024.0;
          cout << FLAGS_packet_size << "," << second << "," << FLAGS_threads << "," << gib << "" << endl;
          sleep(1);
          second++;
        }
        threads_running--;
      });
    // -------------------------------------------------------------------------------------
    for (u64 t_i = 0; t_i < FLAGS_threads; t_i++) {
      threads.emplace_back(
          [&threads_running, &keep_running, &dummy_packet, &threads_cl](u64 t_i) {
            struct sockaddr_in serv_addr;
            int sock = 0;
            if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
              printf("\n Socket creation error \n");
              return;
            }

            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(FLAGS_port);

            // Convert IPv4 and IPv6 addresses from text to binary form
            posix_check(inet_pton(AF_INET, FLAGS_host.c_str(), &serv_addr.sin_addr) > 0);
            posix_check(connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == 0);
            // -------------------------------------------------------------------------------------
            int error = 0;
            socklen_t len = sizeof(error);
            int retval = getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &len);
            if (retval != 0) {
              /* there was a problem getting the error code */
              fprintf(stderr, "error getting socket error code: %s\n", strerror(retval));
              return;
            }
            if (error != 0) {
              /* socket has a non zero error status */
              fprintf(stderr, "socket error: %s\n", strerror(error));
            }
            // -------------------------------------------------------------------------------------
            threads_running++;
            // -------------------------------------------------------------------------------------
            if (FLAGS_profile) {
              CacheLine local_counter;
              PerfEvent e;
              PerfEventBlock b(e, 1);
              while (keep_running) {
                send(sock, dummy_packet.get(), FLAGS_packet_size, 0);
                local_counter.counter += FLAGS_packet_size;
              }
              b.scale = local_counter.counter.load();
            } else {
              while (keep_running) {
                int ret = send(sock, dummy_packet.get(), FLAGS_packet_size, 0);
                posix_check(ret == FLAGS_packet_size);
                threads_cl[t_i].counter += ret;
              }
            }
            close(sock);
            // -------------------------------------------------------------------------------------
            threads_running--;
          },
          threads_counter++);
    }
    while (threads_running < FLAGS_threads) {
    }
    sleep(FLAGS_run_for_seconds);
    keep_running = false;
    while (threads_running) {
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
  return 0;
}
