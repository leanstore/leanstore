import re
import csv

"""
Workload extraction:
raw.txt: The traces collected from `ltrace`:
    `ltrace -C -o output.txt -S git clone --depth 1 git@github.com:torvalds/linux.git`
summary.csv: Analyze raw.txt, then extract & transform the raw commands
    Output into csv format
"""

"""
All SYS_openat flags and meaning:
0x80000 (524288): O_CLOEXEC                                 - 9 times in the traces
0x90800 (591872): O_DIRECTORY | O_NONBLOCK | O_CLOEXEC      - 9 times in the traces
0x800c2 (524482): O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC     - 20 times in the traces
0x80002 (524290): O_CLOEXEC | O_RDWR                        - 1 time in the traces
193: O_WRONLY | O_CREAT | O_EXCL                            - 27 times in the traces
194: O_RDWR | O_CREAT | O_EXCL                              - 1 time in the traces
1089: O_WRONLY | O_CREAT | O_APPEND                         - 3 times in the traces
"""

"""
All SYS_newfstatat flags and meaning:
0: AT_STATX_SYNC_AS_STAT -> default flag, dont
4096: AT_EMPTY_PATH
256: AT_SYMLINK_NOFOLLOW
"""

filters = ["SYS_openat", "SYS_newfstatat", "SYS_close", "SYS_read", "SYS_write"]
filtered_dat = []
re_pattern = "(SYS\_[a-z]*)\((.*)\)\s*\=(.*)"

class Operator:
    def __init__(self, op, fd, size, flags):
        self.op = op
        self.fd = fd
        self.size = size
        self.flags = flags

last_op = None

with open("raw.txt", "r") as inputfile:
    data = [line for line in list(inputfile.readlines()) if line.startswith(tuple(filters)) and "=" in line]
    for line in data:
        x = re.search(re_pattern, line)
        if int(x.group(3)) >= 0:
            params = x.group(2).split(",")
            if x.group(1) == "SYS_openat":
                flags = int(params[2], 16) if params[2].strip().startswith("0x") else int(params[2])
                op = Operator("SYS_openat", int(x.group(3)), 0, flags)
            elif x.group(1) == "SYS_newfstatat":
                if params[0].strip() == "0xffffff9c":
                    params[0] = "100" # Special FD
                op = Operator("SYS_newfstatat", int(params[0]), 0, int(params[3]))
            elif x.group(1) == "SYS_close":
                op = Operator("SYS_close", int(params[0]), 0, 0)
            else:
                op = Operator(x.group(1), int(params[0]), int(params[len(params) - 1]), 0)
                if last_op.op == op.op:
                    last_op.size += op.size
                    continue
            if last_op != None:
                filtered_dat.append(last_op.__dict__)
            last_op = op

filtered_dat.append(last_op.__dict__)

with open("analyzed.csv", "w") as csvfile:
    fieldnames = ["op", "fd", "size", "flags"]
    spamwriter = csv.DictWriter(csvfile, fieldnames, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    spamwriter.writeheader()
    spamwriter.writerows(filtered_dat)
