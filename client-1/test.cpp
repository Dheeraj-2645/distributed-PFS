#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

int main(int argc, char *argv[]) {
    printf("%s:%s: Start! Hostname: %s, IP: %s\n", __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Initialize the PFS client
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "pfs_initialize() failed.\n");
        return -1;
    } else {
        printf("%s:%s: pfs_initialize() succeeded. Client ID: %d\n", __FILE__, __func__, client_id);
    }
    pfs_create("file1",3);
    // Finish the PFS client session
    sleep(100);
    int ret = pfs_finish(client_id);
    if (ret == -1) {
        fprintf(stderr, "pfs_finish() failed.\n");
        return -1;
    } else {
        printf("%s:%s: pfs_finish() succeeded.\n", __FILE__, __func__);
    }

    printf("%s:%s: Finish!\n", __FILE__, __func__);
    return 0;
}
