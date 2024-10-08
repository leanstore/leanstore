#pragma once

#include <array>
#include <string_view>

namespace gitclone {

constexpr auto KB = 1024;

constexpr auto SYSTEM_FILE_INFO =
  std::to_array<std::pair<std::string_view, uint64_t>>({{"/etc/ld.so.cache", 83 * KB},
                                                        {"/lib/x86_64-linux-gnu/libpcre2-8.so.0.11.2", 411 * KB},
                                                        {"/lib/x86_64-linux-gnu/libz.so.1.2.13", 115 * KB},
                                                        {"/lib/x86_64-linux-gnu/libc.so.6", 2025 * KB},
                                                        {"/usr/lib/locale/locale-archive", 14247 * KB},
                                                        {"$HOME/.gitconfig", 1 * KB},
                                                        {"/etc/locale.alias", 3 * KB},
                                                        {"/lib/x86_64-linux-gnu/libcrypto.so.3", 4048 * KB},
                                                        {"/lib/x86_64-linux-gnu/libselinux.so.1", 171 * KB},
                                                        {"/lib/x86_64-linux-gnu/libgssapi_krb5.so.2", 331 * KB},
                                                        {"/lib/x86_64-linux-gnu/libkrb5.so.3.3", 805 * KB},
                                                        {"/lib/x86_64-linux-gnu/libk5crypto.so.3", 175 * KB},
                                                        {"/lib/x86_64-linux-gnu/libcom_err.so.2", 19 * KB},
                                                        {"/lib/x86_64-linux-gnu/libkrb5support.so.0.1", 51 * KB},
                                                        {"/lib/x86_64-linux-gnu/libkeyutils.so.1.10", 23 * KB},
                                                        {"/lib/x86_64-linux-gnu/libresolv.so.2", 63 * KB},
                                                        {"/etc/ssl/openssl.cnf", 13 * KB},
                                                        {"/etc/passwd", 3.1 * KB},
                                                        {"/lib/x86_64-linux-gnu/libnss_systemd.so.2", 323 * KB},
                                                        {"/lib/x86_64-linux-gnu/libcap.so.2.66", 47 * KB},
                                                        {"/lib/x86_64-linux-gnu/libm.so.6", 927 * KB},
                                                        {"/lib/x86_64-linux-gnu/libnss_sss.so.2", 47 * KB},
                                                        {"/lib/x86_64-linux-gnu/libnss_mdns4_minimal.so.2", 19 * KB},
                                                        {"$HOME/.ssh/known_hosts", 4.2 * KB}});

constexpr auto TEMPLATE_FILE_INFO =
  std::to_array<std::pair<std::string_view, uint64_t>>({{"applypatch-msg.sample", 478},
                                                        {"commit-msg.sample", 896},
                                                        {"fsmonitor-watchman.sample", 4.7 * KB},
                                                        {"post-update.sample", 189},
                                                        {"pre-applypatch.sample", 424},
                                                        {"pre-commit.sample", 1.7 * KB},
                                                        {"pre-merge-commit.sample", 416},
                                                        {"prepare-commit-msg.sample", 1.5 * KB},
                                                        {"pre-push.sample", 1.4 * KB},
                                                        {"pre-rebase.sample", 4.8 * KB},
                                                        {"pre-receive.sample", 544},
                                                        {"push-to-checkout.sample", 2.8 * KB},
                                                        {"update.sample", 3.6 * KB}});

}  // namespace gitclone