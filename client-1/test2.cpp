#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

int main(int argc, char *argv[]) {
    printf("Client-2: Start! Hostname: %s, IP: %s\n", getMyHostname().c_str(), getMyIP().c_str());

    // Initialize the PFS client
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client-2: pfs_initialize() failed.\n");
        return -1;
    }

    // Open the existing PFS file in write mode
    int pfs_fd = pfs_open("test_file", 2);
    if (pfs_fd == -1) {
        fprintf(stderr, "Client-2: Error opening PFS file.\n");
        return -1;
    }

    // Request write token for a specific range
    const char *new_data = "Overwriting data with Client-2!";
    size_t data_length = strlen(new_data);
    off_t start_offset = 10; // Example: start writing at offset 10
    char buffer[60];
    int bytes_read=pfs_read(pfs_fd, buffer, data_length, start_offset);
   // Simulate a pfs_read call

    // Check if the read operation was successful
    if (bytes_read == -1) {
        std::cerr << "pfs_read failed!" << std::endl;
        return 0;
    }

    std::cout << "Successfully read " << bytes_read << " bytes from the file." << std::endl;

    // Optionally, check the data in the buffer (e.g., print or compare it with expected data)
    std::cout << "Data read (hex): ";
    for (int i = 0; i < bytes_read; ++i) {
        std::cout << std::hex << (int)buffer[i] << " ";
    }
    std::cout << std::dec << std::endl;  // Switch back to decimal output

    sleep(10); // Adjust sleep time if needed for further testing

    if (pfs_close(pfs_fd) == -1) {
        fprintf(stderr, "Client-2: Error closing PFS file.\n");
        return -1;
    }

    if (pfs_finish(client_id) == -1) {
        fprintf(stderr, "Client-2: pfs_finish() failed.\n");
        return -1;
    }

    printf("Client-2: Finished!\n");
    return 0;
}
