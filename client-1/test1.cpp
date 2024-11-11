#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

int main(int argc, char *argv[]) {
    printf("Client-1: Start! Hostname: %s, IP: %s\n", getMyHostname().c_str(), getMyIP().c_str());

    // Initialize the PFS client
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client-1: pfs_initialize() failed.\n");
        return -1;
    }

    // Create a PFS file
    if (pfs_create("test_file", 3) == -1) {
        fprintf(stderr, "Client-1: Unable to create PFS file.\n");
        return -1;
    }

    // Open the PFS file in write mode
    int pfs_fd = pfs_open("test_file", 2);
    if (pfs_fd == -1) {
        fprintf(stderr, "Client-1: Error opening PFS file.\n");
        return -1;
    }

    // Write dummy data to the PFS file
    const char *dummy_data = "This is some dummy data to test token revocation.";
    size_t data_length = strlen(dummy_data);
    if (pfs_write(pfs_fd, dummy_data, data_length, 0) == -1) {
        fprintf(stderr, "Client-1: Write error.\n");
        return -1;
    }
    printf("Client-1: Wrote dummy data to file.\n");

    // Sleep to keep Client-1 active
    printf("Client-1: Sleeping to allow token revocation...\n");
    sleep(60);  // Sleep for 60 seconds to simulate activity while waiting for Client-2 to request tokens.

    if (pfs_close(pfs_fd) == -1) {
        fprintf(stderr, "Client-1: Error closing PFS file.\n");
        return -1;
    }

    if (pfs_finish(client_id) == -1) {
        fprintf(stderr, "Client-1: pfs_finish() failed.\n");
        return -1;
    }

    printf("Client-1: Finished!\n");
    return 0;
}
