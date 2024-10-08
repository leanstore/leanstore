# Git clone workload

**Command**: `git clone --depth 1 git@github.com:torvalds/linux.git`

## Workload flows

1. All of the system files are read/executed a few times (usually once) during git repo setup
2. Initialize .git directory with the templates in /usr/share/git-core/templates
3. Create a list of all objects to be initialized/cloned
4. Download and write all objects

### Traces

**Dir**: `benchmark/src/gitclone/traces/raw.txt`

The script `main.py` is used to parse `raw.txt` into `analyzed.csv`, which is manually split into three files: `initialize.csv`, `main.csv`, and `end.csv`

### Customizations & Notes

- For step 1, assume that all system files are read only once
- Ignore step 3 because it is unclear & also trivial
- The traces are split into 3 files: `traces/initialize.csv`, `traces/main.csv`, and `traces/end.csv`
  - `traces/initialize.csv` stores the analyzed traces of the 1st and 2nd
  - `traces/main.csv` stores the analyzed traces of the 4th step
  - `traces/end.csv` refers to a few commands to wrap up the process
- The initialization step is actually small and hard to simulate properly, hence we replace it with our own implementation, i.e. just read all system files + copy the template files into our `object` relation
- We complete ignore the end step because it is trivial

**Notes on main workload**:
- We ignore all operator working with stdin/stdout/stderr
  - In fact, only WRITE on stdout is included in main workload
- There are some `fstat` calls on current working dir (i.e. newfstatat(AT_FDCWD, ...) syscall)
  - These are used for checking the `git` run-time files
  - The number of them are small + reliant on `git`` understanding, hence we can ignore that

## System files

- /etc/ld.so.cache:                                      83K
- /lib/x86_64-linux-gnu/libpcre2-8.so.0.11.2:            611K
- /lib/x86_64-linux-gnu/libz.so.1.2.13:                  115K
- /lib/x86_64-linux-gnu/libc.so.6:                       2025K
- /usr/lib/locale/locale-archive:                        14247K
- $HOME/.gitconfig:                                      1K
- /etc/locale.alias:                                     3K
- /lib/x86_64-linux-gnu/libcrypto.so.3:                  4408K
- /lib/x86_64-linux-gnu/libselinux.so.1:                 171K
- /lib/x86_64-linux-gnu/libgssapi_krb5.so.2:             331K
- /lib/x86_64-linux-gnu/libkrb5.so.3.3:                  805K
- /lib/x86_64-linux-gnu/libk5crypto.so.3:                175K
- /lib/x86_64-linux-gnu/libcom_err.so.2:                 19K
- /lib/x86_64-linux-gnu/libkrb5support.so.0.1:           51K
- /lib/x86_64-linux-gnu/libkeyutils.so.1.10:             23K
- /lib/x86_64-linux-gnu/libresolv.so.2:                  63K
- /etc/ssl/openssl.cnf:                                  13K
- /etc/passwd:                                           3.1K
- /lib/x86_64-linux-gnu/libnss_systemd.so.2:             323K
- /lib/x86_64-linux-gnu/libcap.so.2.66:                  47K
- /lib/x86_64-linux-gnu/libm.so.6:                       927K
- /lib/x86_64-linux-gnu/libnss_sss.so.2:                 47K
- /lib/x86_64-linux-gnu/libnss_mdns4_minimal.so.2:       19K
- $HOME/.ssh/known_hosts:                                4.2K

## Template files

**Path**: `/usr/share/git-core/templates`

Only files in subdir `/hooks` matter because all other files are very small and not worth considering.

Files in `/usr/share/git-core/templates/hook`s: 13 files, total of 68K (to be listed later)
- applypatch-msg.sample:      478B
- commit-msg.sample:          896B
- fsmonitor-watchman.sample:  4.7K
- post-update.sample:         189B
- pre-applypatch.sample:      424B
- pre-commit.sample:          1.7K
- pre-merge-commit.sample:    416B
- prepare-commit-msg.sample:  1.5K
- pre-push.sample:            1.4K
- pre-rebase.sample:          4.8K
- pre-receive.sample:         544B
- push-to-checkout.sample:    2.8K
- update.sample:              3.6K
