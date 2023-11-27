# LeanStore as FUSE

**Command**: `./test/LeanStoreFUSE -d -s -f /mnt/test`

**Sample read**
```C++
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>

int main(){
    //./yfs1 is the mount point of my filesystem
    int fd = open("/mnt/test/hello", O_RDONLY);
    auto buf = (char *)malloc(4096);
    int readsize = read(fd, buf, 4096);
    printf("%d,%s,%d\n",fd, buf, readsize);
    close(fd);
}
```