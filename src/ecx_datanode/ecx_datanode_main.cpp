#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <ifaddrs.h>

#include "ych_ec_test.h"
#include "jerasure.h"
#include "reed_sol.h"
#include "galois.h"

char *block_multiply_m = nullptr;
int init[EC_M];
char *buffer_next_ecx_block = nullptr;
int *matrix = nullptr;

/* Global variable control */
int block_count = -1;
pthread_mutex_t mutex_block_count = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_buffer_next_ecx_block = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_block_multiply_m = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t io_mutex = PTHREAD_MUTEX_INITIALIZER;

/* multiple threads control */
pthread_mutex_t cond_net_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t cond_enc_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t cond_request_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t cond_ecx_request_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t cond_request_ecx_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t cond_net = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_enc = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_request = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_request_ecx = PTHREAD_COND_INITIALIZER;

int cur_block_net;
int cur_block_enc;
int cur_block_request;
int cur_block_request_ecx;
int cur_eck_net = 0;
int cur_eck_enc = 0;
int ecm;

int replace_filename_suffix(char *filename, int suffix)
{
    // Find the position of the last '-' character
    char *dash_pos = strrchr(filename, '_');
    if (dash_pos == NULL)
    {
        printf("[replace_filename_suffix] Invalid filename\n");
        return EC_ERROR;
    }

    // Find the position of the character to replace
    int pos = dash_pos - filename + 1;

    // Replace the number with the new number
    char new_num_str[12];
    sprintf(new_num_str, "%d", suffix);
    strcpy(&filename[pos], new_num_str);

    return EC_OK;
}

int get_local_ip_lastnum(int *lastnum_p)
{
    struct ifaddrs *ifAddrStruct = NULL;
    struct ifaddrs *ifa = NULL;
    void *tmpAddrPtr = NULL;

    getifaddrs(&ifAddrStruct);
    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (ifa->ifa_addr->sa_family == AF_INET)
        { // IPv4 address
            tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            if (strncmp(addressBuffer, IP_PREFIX, 10) == 0)
            {
                char *lastNumStr = strrchr(addressBuffer, '.') + 1;
                *lastnum_p = atoi(lastNumStr);
                return EC_OK;
            }
        }
    }

    if (ifAddrStruct != NULL)
        freeifaddrs(ifAddrStruct);
    return EC_ERROR;
}

int initialize_network(int *sockfd_p, int port, int ip_offset)
{
    struct sockaddr_in server_addr;
    int ip_addr_start = DATANODE_START_IP_ADDR;
    int sockfd;

    /* Create socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1)
    {
        printf("[initialize_network] Failed to create socket\n");
        return EC_ERROR;
    }

    /* Create sockaddr */
    char ip_addr[16];
    sprintf(ip_addr, "%s%d", IP_PREFIX, ip_addr_start + ip_offset);
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip_addr, &server_addr.sin_addr) <= 0)
    {
        printf("[initialize_network] Invalid IP address\n");
        return EC_ERROR;
    }

    /* Connect data datanode of ip_addr */
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        printf("[initialize_network] Error connecting to %s\n", ip_addr);
        return EC_ERROR;
    }
    *sockfd_p = sockfd;
    return EC_OK;
}

void *handle_block_file_io(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;

    if (block_count == -1) // If is the first round block
    {
        pthread_mutex_lock(&mutex_block_count);
        block_count = 0;
        pthread_mutex_unlock(&mutex_block_count);
    }

    /* Prepare-offset */
    long int offset = metadata->chunk_size;

    /* Prepare-filename */
    int tmp_return = replace_filename_suffix(metadata->dst_filename_datanode, ecm + 1);
    if (tmp_return == EC_ERROR)
    {
        printf("[handle_block_file_io] Failed to replace_filename_suffix\n");
        return nullptr;
    }

    /* write block to disk */
    FILE *chunk_fp = fopen(metadata->dst_filename_datanode, "r+");
    if (chunk_fp == NULL)
    {
        chunk_fp = fopen(metadata->dst_filename_datanode, "w+");
        if (chunk_fp == NULL)
        {
            printf("[handle_block_file_io] Failed to open dst_filename_datanode file\n");

            return nullptr;
        }
    }

    pthread_mutex_lock(&io_mutex);
#if (DISK_WRITE_TEST)
    struct timeval t_io1, t_io2;
    struct timezone tz;
    double tsec;
    gettimeofday(&t_io1, &tz);
#endif

    fseek(chunk_fp, offset * sizeof(char), SEEK_SET);
    if (fwrite(metadata->data, sizeof(char), (size_t)metadata->block_size, chunk_fp) != (size_t)metadata->block_size)
    {
        printf("[handle_block_file_io] Failed to write dst_filename_datanode file\n");
        return nullptr;
    }
    fflush(chunk_fp);
    fsync(fileno(chunk_fp));
    fclose(chunk_fp);

#if (DISK_WRITE_TEST)
    gettimeofday(&t_io2, &tz);
    tsec = 0.0;
    tsec += t_io2.tv_usec;
    tsec -= t_io1.tv_usec;
    tsec /= 1000000.0;
    tsec += t_io2.tv_sec;
    tsec -= t_io1.tv_sec;
    int sleep_time = (int)(tsec * 1000000 * (DISK_DELAY_MUL - 1));
#if (TEST_LOG)
    printf("io time = %0.10f, sleep_time = %d\n", tsec, sleep_time);
#endif
    usleep(sleep_time);
#endif
    pthread_mutex_unlock(&io_mutex);

    pthread_mutex_lock(&mutex_block_count);
    block_count++;
    pthread_mutex_unlock(&mutex_block_count);

    if (block_count == EC_N) // Only cur_block is last block
    {
        pthread_mutex_lock(&mutex_block_count);
        block_count = -1;
        pthread_mutex_unlock(&mutex_block_count);

        /* Send one chunk ok to client */
        tmp_return = initialize_network(&metadata->sockfd, EC_WRITE_PORT, -1);
        if (tmp_return == EC_ERROR)
        {
            printf("[handle_client_write_new] Failed to initialize network: Send one chunk ok to client\n");
            return nullptr;
        }
        /* Send chunk ok and recv response */
        int chunk_ok = 1;
        if (send(metadata->sockfd, &chunk_ok, sizeof(chunk_ok), 0) < 0)
        {
            printf("[handle_client_write_new] Failed to send block metadata to datanode\n");
            metadata->error_flag = EC_ERROR;
            return nullptr;
        }
        int error_response = 0;
        if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
        {
            printf("[handle_client_write_new] Failed to recv response \n");
            metadata->error_flag = EC_ERROR;
            return nullptr;
        }
        if (error_response == 0)
        {
            printf("[handle_client_write_new] Failed to recv response \n");
            metadata->error_flag = EC_ERROR;
            return nullptr;
        }
        close(metadata->sockfd);
    }
    free(metadata->data);
    free(metadata);
    return nullptr;
}

void *handle_file_io(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;
    /* write chunk to disk */
    FILE *chunk_fp = fopen(metadata->dst_filename_datanode, "wb");
    if (chunk_fp == NULL)
    {
        printf("[handle_file_io] Failed to open dst_filename_datanode file\n");
        return nullptr;
    }
    if (fwrite(metadata->data, sizeof(char), (size_t)metadata->chunk_size, chunk_fp) != (size_t)metadata->chunk_size)
    {
        printf("[handle_file_io] Failed to write dst_filename_datanode file\n");
        return nullptr;
    }
    fflush(chunk_fp);
    fsync(fileno(chunk_fp));
    fclose(chunk_fp);
    return nullptr;
}

void *send_one_request_datanode(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;

    /* Send block metadata */
    if (send(metadata->sockfd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[send_one_request_datanode] Failed to send metadata to datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    int error_response = 0;
    if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[send_one_request_datanode] Failed to recv metadata response from datanode\n");
        perror("recv");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    if (error_response == 0)
    {
        printf("[send_one_request_datanode]  Failed to recv metadata response from datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }

    /*Recv ecx block */
    int save_block_size;
    long int save_offset;
    if (metadata->cur_block - 1 == 0)
    {
        save_offset = 0;
        save_block_size = metadata->block_size + metadata->remain_block_size;
    }
    else
    {
        save_offset = metadata->remain_block_size + (metadata->cur_block - 1) * metadata->block_size;
        save_block_size = metadata->block_size;
    }

#if (NET_BANDWIDTH_MODE || ENCODE_ISOMERISM_MODE)
    if (metadata->cur_block - 1 != 0)
    {
        int i;
        int sum_block_size = 0;
        int rounds_num = (metadata->cur_block - 1) / EC_X;
        for (i = 0; i < EC_X; i++)
        {
            sum_block_size += metadata->net_block_size[i];
        }
        sum_block_size *= rounds_num;
        int remain_block_num = (metadata->cur_block - 1) % EC_X;

        if (remain_block_num != 0)
        {
            for (i = 0; i < remain_block_num; i++)
            {
                sum_block_size += metadata->net_block_size[i];
            }
        }
        save_offset = metadata->remain_block_size + sum_block_size;
    }

    if (metadata->cur_block - 1 == 0)
    {
        save_block_size = metadata->net_block_size[0] + metadata->remain_block_size;
    }
    else
    {
        save_block_size = metadata->net_block_size[(metadata->cur_block - 1) % EC_X];
    }
#endif

    char *save_block = (char *)malloc(sizeof(char) * save_block_size);
    char *tmp_buffer_block = save_block;
    int tmp_block_size = save_block_size;
    int recv_size;
    while (tmp_block_size > 0)
    {
        recv_size = recv(metadata->sockfd, tmp_buffer_block, tmp_block_size, 0);
        if (recv_size < 0)
        {
            printf("[send_one_request_datanode] Failed to recv chunc data\n");
            perror("recv"); // print error information
            metadata->error_flag = EC_ERROR;
            return nullptr;
        }
        tmp_block_size -= recv_size;
        tmp_buffer_block += recv_size;
    }
    error_response = 0;
    if (send(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[send_one_request_datanode] Failed to send response\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    // close(metadata->sockfd);

    metadata_t *save_metadata = (metadata_t *)malloc(sizeof(metadata_t));
    memcpy(save_metadata, metadata, sizeof(metadata_t));
    save_metadata->data = save_block;
    save_metadata->chunk_size = save_offset;
    save_metadata->block_size = save_block_size;

    pthread_t save_tid;
    if (pthread_create(&save_tid, NULL, handle_block_file_io, (void *)save_metadata) != 0)
    {
        printf("[send_one_request_datanode] Failed to create IO thread\n");
        return nullptr;
    }

    return nullptr;
}

void *send_one_block_datanode(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;

    /* Send block metadata and recv response */
    if (send(metadata->sockfd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[send_one_block_datanode] Failed to send block metadata to datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    int error_response = 0;
    if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[send_one_block_datanode] 1Failed to recv block metadata response from datanode\n");
        perror("recv");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    if (error_response == 0)
    {
        printf("[send_one_block_datanode] 2Failed to recv block metadata response from datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }

    /* Send block data and recv response */
    if (send(metadata->sockfd, metadata->data, metadata->block_size, 0) < 0)
    {
        printf("[send_one_block_datanode] Failed to send block data to datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }

    if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[send_one_block_datanode] Failed to recv block data response from datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    if (error_response == 1)
    {
        printf("[send_one_block_datanode] Failed to recv block data response from datanode: recv error\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    close(metadata->sockfd);
    return nullptr;
}

int server_initialize_network(int *server_fd_p, int port)
{
    int server_fd;
    struct sockaddr_in server_addr;

    /* create socket */
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        printf("[datanode_initialize network] Failed to create socket\n");
        return EC_ERROR;
    }
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    /* bind address and port */
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1)
    {
        printf("[datanode_initialize network] Failed to bind socket\n");
        return EC_ERROR;
    }

    /* listen socket */
    if (listen(server_fd, 5) == -1)
    {
        printf("[datanode_initialize network] Failed to listen socket\n");
        return EC_ERROR;
    }
    *server_fd_p = server_fd;
    return EC_OK;
}

void *handle_client_write_ecx(void *arg)
{
    int client_fd = *((int *)arg);
    int tmp_return;
    metadata_t *metadata = (metadata_t *)malloc(sizeof(metadata_t));
    int error_response = 0;
    /* Recv metadata */
    if (recv(client_fd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[handle_client_write_ecx] Failed to recv metadata\n");
        return nullptr;
    }

    if (metadata->cur_eck != -1) // Print information
    {
        printf("[handle_client_write_ecx] error recv cur_eck = %d\n", metadata->cur_eck);
        return nullptr;
    }
#if (TEST_LOG)
    else
    {

        printf("[handle_client_write_ecx] RECV ECX BLOCK :CUR_ECK = %d CUR_BLOCK = %d\n", metadata->cur_eck, metadata->cur_block);
    }
#endif

    error_response = 1;
    if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_client_write_ecx] Failed to send metadata response to ecx datanode\n");
        return nullptr;
    }

    char *save_block = (char *)malloc(sizeof(char) * metadata->block_size);
    long int save_offset;

    if (metadata->cur_block == 0)
    {
        save_offset = 0;
    }
    else
    {
        save_offset = metadata->remain_block_size + metadata->cur_block * metadata->block_size;
    }

#if (NET_BANDWIDTH_MODE || ENCODE_ISOMERISM_MODE)
    if (metadata->cur_block != 0)
    {
        int sum_block_size = 0;
        int rounds_num = metadata->cur_block / EC_X;
        int i;
        for (i = 0; i < EC_X; i++)
        {
            sum_block_size += metadata->net_block_size[i];
        }
        sum_block_size *= rounds_num;
        int remain_block_num = metadata->cur_block % EC_X;

        if (remain_block_num != 0)
        {
            for (i = 0; i < remain_block_num; i++)
            {
                sum_block_size += metadata->net_block_size[i];
            }
        }
        save_offset = metadata->remain_block_size + sum_block_size;
    }
#endif

    /* recv ecx block data */
    int recv_size;
    int tmp_block_size = metadata->block_size;
    char *tmp_buffer_block = save_block;
    while (tmp_block_size > 0)
    {
        recv_size = recv(client_fd, tmp_buffer_block, tmp_block_size, 0);
        if (recv_size < 0)
        {
            printf("[handle_client_write_ecx] Failed to recv ecx block data\n");
            return nullptr;
        }
        tmp_block_size -= recv_size;
        tmp_buffer_block += recv_size;
    }
    error_response = 0;
    if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_client_write_ecx] Failed to send block data response to ecx datanode\n");
        return nullptr;
    }

    metadata_t *save_metadata = (metadata_t *)malloc(sizeof(metadata_t));
    memcpy(save_metadata, metadata, sizeof(metadata_t));
    save_metadata->data = save_block;
    save_metadata->chunk_size = save_offset;

    pthread_t save_tid;
    if (pthread_create(&save_tid, NULL, handle_block_file_io, (void *)save_metadata) != 0)
    {
        printf("[send_one_request_datanode] Failed to create IO thread\n");
        return nullptr;
    }

    close(client_fd);
    free(metadata);
    return nullptr;
}

void *handle_client_write_request(void *arg)
{
    int client_fd = *((int *)arg);
    metadata_t *metadata = (metadata_t *)malloc(sizeof(metadata_t));
    int tmp_block_size;
    int error_response = 0;

    /* Recv metadata */
    if (recv(client_fd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[handle_client_write_request] Failed to recv metadata\n");
        return nullptr;
    }
    error_response = 1;
    if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_client_write_request] Failed to send response\n");
        return nullptr;
    }

    if (metadata->cur_eck != -2) // Print information
    {
        printf("[handle_client_write_request] error recv\n");
        return nullptr;
    }
#if (TEST_LOG)
    else
    {
        printf("[handle_client_write_request] RECV ECX REQUEST BLOCK :CUR_ECK = %d CUR_BLOCK = %d\n", metadata->cur_eck, metadata->cur_block);
    }
#endif
    pthread_mutex_lock(&cond_request_mutex);
    while (metadata->cur_block != cur_block_request + 1)
    {
        pthread_cond_wait((pthread_cond_t *)&cond_request, &cond_request_mutex);
    }

    /* Send ecx block */
    if (metadata->cur_block - 1 == 0)
    {
        tmp_block_size = metadata->block_size + metadata->remain_block_size;
    }
    else
    {
        tmp_block_size = metadata->block_size;
    }

#if (NET_BANDWIDTH_MODE || ENCODE_ISOMERISM_MODE)
    if (metadata->cur_block - 1 == 0)
    {
        tmp_block_size = metadata->net_block_size[0] + metadata->remain_block_size;
    }
    else
    {
        tmp_block_size = metadata->net_block_size[(metadata->cur_block - 1) % EC_X];
    }
#endif

    if (send(client_fd, buffer_next_ecx_block, (size_t)tmp_block_size, 0) < 0)
    {
        printf("[handle_client_write_request] Failed to send ecx request block\n");
        return nullptr;
    }
    error_response = 1;
    if (recv(client_fd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_client_write_request] Failed to recv response\n");
        return nullptr;
    }
    if (error_response == 1)
    {
        printf("[send_one_block_datanode] Failed to recv response: recv error\n");
        return nullptr;
    }

    free(buffer_next_ecx_block);
    pthread_mutex_unlock(&cond_request_mutex);
    pthread_mutex_unlock(&mutex_buffer_next_ecx_block);

    close(client_fd);
    free(metadata);
    return nullptr;
}

void *handle_client_write_new_enc(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;
    char *buffer_block = metadata->data;
    int i;
    int tmp_return;
    pthread_mutex_lock(&cond_enc_mutex);
    while (metadata->cur_eck != cur_eck_enc || metadata->cur_block != cur_block_enc)
    {
        pthread_cond_wait((pthread_cond_t *)&cond_enc, &cond_enc_mutex);
    }

    /* For encode and save */
    // if (metadata->cur_eck == 0) // If eck block from the first eck datanode
    if (cur_eck_enc == 0)
    {
        pthread_mutex_lock(&mutex_block_multiply_m);
        block_multiply_m = (char *)malloc(sizeof(char) * (metadata->block_size + sizeof(long)) * EC_M); // Save intermediate coding block during multiplication and addition calculations
        for (i = 0; i < EC_M; i++)                                                                      // For the first multiplication calculation, no addition calculation
        {
            init[i] = 0;
        }
    }
    char **coding_block = (char **)malloc(sizeof(char *) * EC_M); // Convenient to save intermediate coding block

    /* Encoded */
    int matrix_value[EC_M];
#if (ENCODE_ISOMERISM_MODE || ENCODE_WRITE_TEST)
    struct timeval enc_start, enc_end;
    double enc_seconds = 0.0;
    gettimeofday(&enc_start, NULL);
#endif
    for (i = 0; i < EC_M; i++)
    {
        matrix_value[i] = *(matrix + (i * EC_K) + metadata->cur_eck);
        coding_block[i] = block_multiply_m + i * (metadata->block_size + sizeof(long));

        /* First copy or xor any data that does not need to be multiplied by a factor */
        if (matrix_value[i] == 1)
        {
            if (init[i] == 0)
            {
                memcpy(coding_block[i], buffer_block, metadata->block_size);
                init[i] = 1;
            }
            else
            {
                galois_region_xor(buffer_block, coding_block[i], coding_block[i], metadata->block_size);
            }
        }

        /* Now do the data that needs to be multiplied by a factor */
        if (matrix_value[i] != 0 && matrix_value[i] != 1)
        {
            switch (EC_W)
            {
            case 8:
                galois_w08_region_multiply(buffer_block, matrix_value[i], metadata->block_size, coding_block[i], init[i]); // init这里首次进入不会加，第二次进入会加
                break;
            case 16:
                galois_w16_region_multiply(buffer_block, matrix_value[i], metadata->block_size, coding_block[i], init[i]);
                break;
            case 32:
                galois_w32_region_multiply(buffer_block, matrix_value[i], metadata->block_size, coding_block[i], init[i]);
                break;
            }
            init[i] = 1;
        }
    }
    free(buffer_block);
    free(coding_block);
#if (ENCODE_ISOMERISM_MODE || ENCODE_WRITE_TEST)
    gettimeofday(&enc_end, NULL); // compute the time of encoding
    enc_seconds += enc_start.tv_usec;
    enc_seconds -= enc_end.tv_usec;
    enc_seconds /= 1000000.0;
    enc_seconds += enc_start.tv_sec;
    enc_seconds -= enc_end.tv_sec;
#if (ENCODE_WRITE_TEST)
    int sleep_time = (int)(enc_seconds * 1000000 * (ENCODE_DELAY_MUL - 1));
#endif
#if (ENCODE_ISOMERISM_MODE)
    int sleep_time = (int)(enc_seconds * 1000000 * (eiRatio[ecm - EC_K] - 1));
#endif

#if (TEST_LOG)
    printf("block enc time = %0.10f, sleep_time = %d\n", enc_seconds, sleep_time);
#endif
    usleep(sleep_time);
#endif
    char *tmp_block_multiply_m = nullptr;
    if (cur_eck_enc == EC_K - 1)
    {
        tmp_block_multiply_m = (char *)malloc(sizeof(char) * (metadata->block_size + sizeof(long)) * EC_M); // Avoid being used by other threads before the coding blocks are transmitted completely
        memcpy(tmp_block_multiply_m, block_multiply_m, (metadata->block_size + sizeof(long)) * EC_M);
        free(block_multiply_m);

        pthread_mutex_unlock(&mutex_block_multiply_m);
    }

    if (cur_eck_enc == EC_K - 1)
    {
        cur_eck_enc = 0;
        cur_block_enc = cur_block_enc + EC_X >= EC_N ? ecm - EC_K : cur_block_enc + EC_X;
    }
    else
    {
        cur_eck_enc++;
    }

    pthread_cond_broadcast(&cond_enc);
    pthread_mutex_unlock(&cond_enc_mutex);

    /* If is last cur eck block */
    if (metadata->cur_eck == EC_K - 1)
    {
        /* Prepare */
        int next_next_ecx_datanode = (metadata->cur_block + 2) % EC_X;
        int next_ecx_datanode = (metadata->cur_block + 1) % EC_X;
        int local_ecx_datanode = metadata->cur_block % EC_X;
        int prev_ecx_datanode = (metadata->cur_block - 1 + EC_X) % EC_X;

        /* Save coding block */
        if (local_ecx_datanode < EC_M)
        {
            char *save_block = (char *)malloc(sizeof(char) * metadata->block_size);
            long int save_offset;
            if (metadata->cur_block == 0)
            {
                save_offset = 0;
            }
            else
            {
                save_offset = metadata->remain_block_size + metadata->cur_block * metadata->block_size;
            }

#if (NET_BANDWIDTH_MODE || ENCODE_ISOMERISM_MODE)
            if (metadata->cur_block != 0)
            {
                int sum_block_size = 0;
                int rounds_num = metadata->cur_block / EC_X;
                for (i = 0; i < EC_X; i++)
                {
                    sum_block_size += metadata->net_block_size[i];
                }
                sum_block_size *= rounds_num;
                int remain_block_num = metadata->cur_block % EC_X;

                if (remain_block_num != 0)
                {
                    for (i = 0; i < remain_block_num; i++)
                    {
                        sum_block_size += metadata->net_block_size[i];
                    }
                }
                save_offset = metadata->remain_block_size + sum_block_size;
            }
#endif
            memcpy(save_block, tmp_block_multiply_m + local_ecx_datanode * (metadata->block_size + sizeof(long)), metadata->block_size);

            metadata_t *save_metadata = (metadata_t *)malloc(sizeof(metadata_t));
            memcpy(save_metadata, metadata, sizeof(metadata_t));
            save_metadata->data = save_block;
            save_metadata->chunk_size = save_offset;

            pthread_t save_tid;
            if (pthread_create(&save_tid, NULL, handle_block_file_io, (void *)save_metadata) != 0)
            {
                printf("[handle_client_write_new] Failed to create IO thread\n");
                return nullptr;
            }
        }

        pthread_mutex_lock(&cond_request_ecx_mutex);
        while (metadata->cur_block != cur_block_request_ecx)
        {
            pthread_cond_wait((pthread_cond_t *)&cond_request_ecx, &cond_request_ecx_mutex);
        }
#if (EC_X > 1)
#if (EC_X <= EC_M)
        /* Save for next ecx datanode */
        if (metadata->cur_block != EC_N - 1)
#else
        if (metadata->cur_block != EC_N - 1 && (local_ecx_datanode < EC_M - 1 || local_ecx_datanode == EC_X - 1))
#endif
        {
            pthread_mutex_lock(&mutex_buffer_next_ecx_block);
            buffer_next_ecx_block = (char *)malloc(sizeof(char) * metadata->block_size);
            memcpy(buffer_next_ecx_block, tmp_block_multiply_m + next_ecx_datanode * (metadata->block_size + sizeof(long)), metadata->block_size);
            cur_block_request = cur_block_request + EC_X >= EC_N - 1 ? ecm - EC_K : cur_block_request + EC_X;
            pthread_cond_broadcast(&cond_request);
        }

        /* Send coding blocks request to previous ecx datanode */
        pthread_t tid_request;
        metadata_t *request_metadata;
        if (metadata->cur_block != 0 && local_ecx_datanode < EC_M) // No the first block and is coding datanode
        {
            request_metadata = (metadata_t *)malloc(sizeof(metadata_t));
            memcpy(request_metadata, metadata, sizeof(metadata_t));
            int tmp_return = initialize_network(&request_metadata->sockfd, EC_WRITE_REQUEST_BASE_PORT + (ecm - EC_K - 1 + EC_X) % EC_X, EC_K + prev_ecx_datanode);
            if (tmp_return == EC_ERROR)
            {
                printf("[send_one_request_datanode] Failed to initialize network\n");
                return nullptr;
            }
            request_metadata->cur_eck = -2; // ecx block request
            if (pthread_create(&tid_request, NULL, send_one_request_datanode, (void *)request_metadata) != 0)
            {
                printf("[handle_client_write_new] Failed to create send one block thread\n");
                return nullptr;
            }
        }
#endif

        /* Send coding blocks to ecm datanode, not send to coding datanode that also is next ecx datanode */
#if (EC_X <= EC_M)
        if (metadata->cur_block == EC_N - 1) // The last block, send all other coding datanodes
        {
            i = next_ecx_datanode;
        }
        else
        {
            i = next_next_ecx_datanode;
        }

        while (i != local_ecx_datanode) // First send to the next ecx datanode
        {
            tmp_return = initialize_network(&metadata->sockfd, EC_WRITE_ECX_BASE_PORT + local_ecx_datanode, EC_K + i);
            if (tmp_return == EC_ERROR)
            {
                printf("[handle_client_write_new] Failed to initialize EC_WRITE_ECX_PORT network\n");
                return nullptr;
            }
            metadata->data = tmp_block_multiply_m + i * (metadata->block_size + sizeof(long));
            metadata->cur_eck = -1; // ecx block
            pthread_t tid_block;
            if (pthread_create(&tid_block, NULL, send_one_block_datanode, (void *)metadata) != 0)
            {
                printf("[handle_client_write_new] Failed to create send one block thread\n");
                return nullptr;
            }
            /* Wait until thread end */
            if (pthread_join(tid_block, nullptr) != 0)
            {
                printf("[handle_client_write_new] Failed to join thread\n");
                return nullptr;
            }
            close(metadata->sockfd);
            i++;
            if (i >= EC_X)
            {
                i = i % EC_X;
            }
        }

#if (EC_X < EC_M)
        for (i = EC_X; i < EC_M; i++) // Then send to ecm datanode
        {
            /* Initialize thread metadata */
            tmp_return = initialize_network(&metadata->sockfd, EC_WRITE_ECX_BASE_PORT + local_ecx_datanode, EC_K + i);
            if (tmp_return == EC_ERROR)
            {
                printf("[handle_client_write_new] Failed to initialize network\n");
                return nullptr;
            }
            metadata->data = tmp_block_multiply_m + i * (metadata->block_size + sizeof(long));
            metadata->cur_eck = -1; // ecx block
            pthread_t tid_block;
            if (pthread_create(&tid_block, NULL, send_one_block_datanode, (void *)metadata) != 0)
            {
                printf("[handle_client_write_new] Failed to create send one block thread\n");
                return nullptr;
            }

            /* Wait until thread end */
            if (pthread_join(tid_block, nullptr) != 0)
            {
                printf("[handle_client_write_new] Failed to join thread\n");
                return nullptr;
            }
            close(metadata->sockfd);
        }
#endif
#else
        if (local_ecx_datanode < EC_M)
        {
            if (local_ecx_datanode + 1 == EC_M || metadata->cur_block == EC_N - 1) // The last coding datanode or the last block, send all other coding datanodes
            {
                i = (local_ecx_datanode + 1) % EC_M;
            }
            else
            {
                i = (local_ecx_datanode + 2) % EC_M;
            }

            /* First send to ecm datanode*/
            while (i != local_ecx_datanode)
            {
                /* Initialize thread metadata */
                tmp_return = initialize_network(&metadata->sockfd, EC_WRITE_ECX_BASE_PORT + local_ecx_datanode, EC_K + i);
                if (tmp_return == EC_ERROR)
                {
                    printf("[handle_client_write_new] Failed to initialize network: First send to ecm datanode\n");
                    return nullptr;
                }
                metadata->data = tmp_block_multiply_m + i * (metadata->block_size + sizeof(long));
                metadata->cur_eck = -1; // ecx block
                pthread_t tid_block;
                if (pthread_create(&tid_block, NULL, send_one_block_datanode, (void *)metadata) != 0)
                {
                    printf("[handle_client_write_new] Failed to create send one block thread\n");
                    return nullptr;
                }

                /* Wait until thread end */
                if (pthread_join(tid_block, nullptr) != 0)
                {
                    printf("[handle_client_write_new] Failed to join thread\n");
                    return nullptr;
                }
                close(metadata->sockfd);
                i++;
                if (i >= EC_M)
                {
                    i = i % EC_M;
                }
            }
        }
        else
        {
            /* Send coding blocks to ecm datanode */
            for (i = 0; i < EC_M; i++)
            {
                if (local_ecx_datanode == EC_X - 1 && i == 0 && metadata->cur_block != EC_N - 1) // Next ecx datanode is coding datanode, should skip
                {
                    continue;
                }
                /* Initialize thread metadata */
                tmp_return = initialize_network(&metadata->sockfd, EC_WRITE_ECX_BASE_PORT + local_ecx_datanode, EC_K + i);
                if (tmp_return == EC_ERROR)
                {
                    printf("[handle_client_write_new] Failed to initialize network: Send coding blocks to ecm datanode\n");
                    return nullptr;
                }
                metadata->data = tmp_block_multiply_m + i * (metadata->block_size + sizeof(long));
                metadata->cur_eck = -1; // ecx block
                pthread_t tid_block;
                if (pthread_create(&tid_block, NULL, send_one_block_datanode, (void *)metadata) != 0)
                {
                    printf("[handle_client_write_new] Failed to create send one block thread\n");
                    return nullptr;
                }

                /* Wait until thread end */
                if (pthread_join(tid_block, nullptr) != 0)
                {
                    printf("[handle_client_write_new] Failed to join thread\n");
                    return nullptr;
                }
                close(metadata->sockfd);
            }
        }
#endif

#if (EC_X > 1)
        /* Send ECX block and request ECX block in parallel */
        if (metadata->cur_block != 0 && local_ecx_datanode < EC_M) // Not the first block and is coding datanode
        {
            /* Wait until thread end */
            if (pthread_join(tid_request, nullptr) != 0)
            {
                printf("[handle_client_write_new] Failed to join thread\n");
                return nullptr;
            }
            close(request_metadata->sockfd);
            free(request_metadata);
        }
#endif
        cur_block_request_ecx = cur_block_request_ecx + EC_X >= EC_N ? ecm - EC_K : cur_block_request_ecx + EC_X;
        pthread_cond_broadcast(&cond_request_ecx);
        pthread_mutex_unlock(&cond_request_ecx_mutex);

        free(tmp_block_multiply_m);
    }
    free(metadata);
    return nullptr;
}

void *handle_client_write_new(void *arg)
{
    int client_fd = *((int *)arg);
    while (1)
    {
        int i;
        int tmp_return;
        metadata_t *metadata = (metadata_t *)malloc(sizeof(metadata_t));
        int error_response = 0;
        /* Recv metadata and send response */
        if (recv(client_fd, metadata, sizeof(metadata_t), 0) < 0)
        {
            printf("[handle_client_write_new] Failed to recv metadata\n");
            return nullptr;
        }

        error_response = 1;
        if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
        {
            printf("[handle_client_write_new] Failed to send metadata response\n");
            return nullptr;
        }

        /* If recv eck block */
        if (metadata->cur_eck > -1 && metadata->cur_eck < EC_K)
        {
            pthread_mutex_lock(&cond_net_mutex);
            while (metadata->cur_eck != cur_eck_net || metadata->cur_block != cur_block_net)
            {
                pthread_cond_wait((pthread_cond_t *)&cond_net, &cond_net_mutex);
            }
#if (TEST_LOG)
            printf("[handle_client_write_new] RECV ECK BLOCK: CUR_ECK = %d CUR_BLOCK = %d\n", metadata->cur_eck, metadata->cur_block); // Print information
#endif
            /* recv eck block data and send response */
            char *buffer_block = (char *)malloc(sizeof(char) * metadata->block_size); // Save recv block, not conflict with other threads
            int recv_size;
            int tmp_block_size = metadata->block_size;
            char *tmp_buffer_block = buffer_block;
            while (tmp_block_size > 0)
            {
                recv_size = recv(client_fd, tmp_buffer_block, tmp_block_size, 0);
                if (recv_size < 0)
                {
                    printf("[handle_client_write_new] Failed to recv eck block data\n");
                    return nullptr;
                }
                tmp_block_size -= recv_size;
                tmp_buffer_block += recv_size;
            }
            error_response = 0;
            if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
            {
                printf("[handle_client_write_new] Failed to send block data response to eck datanode\n");
                return nullptr;
            }

            if (cur_eck_net == EC_K - 1)
            {
                cur_eck_net = 0;
                cur_block_net = cur_block_net + EC_X >= EC_N ? ecm - EC_K : cur_block_net + EC_X;
            }
            else
            {
                cur_eck_net++;
            }
            metadata->data = buffer_block;
            pthread_t tid_enc;
            if (pthread_create(&tid_enc, NULL, handle_client_write_new_enc, (void *)metadata) != 0)
            {
                printf("[handle_client_write_new] Failed to create enc thread\n");
                return nullptr;
            }

            pthread_cond_broadcast(&cond_net);
            pthread_mutex_unlock(&cond_net_mutex);
        }
        else
        {
            printf("[handle_client_write_new] error recv: CUR_ECK = %d CUR_BLOCK = %d\n", metadata->cur_eck, metadata->cur_block);
            return nullptr;
        }
        if (metadata->cur_block >= EC_N - EC_X)
        {
            break;
        }
    }
    close(client_fd);
    return nullptr;
}

void *handle_client_write(void *arg)
{
    int client_fd = *((int *)arg);
    int tmp_return;
    metadata_t *metadata = (metadata_t *)malloc(sizeof(metadata_t));
    int error_response = 0;
    /* Recv metadata and send response */
    if (recv(client_fd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[handle_client_write] Failed to recv metadata\n");
        return nullptr;
    }

    error_response = 1;
    if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_client_write] Failed to send metadata response\n");
        return nullptr;
    }
    /* If recv chunk */
    if (metadata->block_size == -1)
    {
        /* recv chunk data and send response */
        char *buffer_chunk_regular = (char *)malloc(sizeof(char) * metadata->chunk_size); // buffer for EC chunk
        int recv_size;
        int tmp_chunk_size = metadata->chunk_size;
        char *tmp_buffer_chunk = buffer_chunk_regular;
        while (tmp_chunk_size > 0)
        {
            recv_size = recv(client_fd, tmp_buffer_chunk, tmp_chunk_size, 0);
            if (recv_size < 0)
            {
                printf("[handle_client_write] Failed to recv chunc data\n");
                return nullptr;
            }
            tmp_chunk_size -= recv_size;
            tmp_buffer_chunk += recv_size;
        }
        error_response = 0;
        if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
        {
            printf("[handle_client_write] Failed to send chunk data response to client\n");
            return nullptr;
        }

        /* create thread to handle file IO */
        pthread_t tid;
        metadata->data = buffer_chunk_regular;
        if (pthread_create(&tid, NULL, handle_file_io, (void *)metadata) != 0)
        {
            printf("[handle_client_write] Failed to create IO thread\n");
            return nullptr;
        }
        /* Wait until thread end */
        if (pthread_join(tid, nullptr) != 0)
        {
            printf("[handle_client_write] Failed to join thread\n");
            return nullptr;
        }
        free(buffer_chunk_regular);
    }
    else
    {
        printf("[handle_client_write_new] error recv: CUR_ECK = %d CUR_BLOCK = %d\n", metadata->cur_eck, metadata->cur_block);
        return nullptr;
    }
    close(client_fd);
    free(metadata);
    return nullptr;
}
void *handle_client_read(void *arg)
{
    int client_fd = *((int *)arg);
    metadata_t *metadata = (metadata_t *)malloc(sizeof(metadata_t));
    int recv_return;

    /* recv chunk metadata and send response */
    if ((recv_return = recv(client_fd, metadata, sizeof(metadata_t), 0)) < 0)
    {
        printf("[handle_client_read] Failed to recv chunk metadata\n");
        return nullptr;
    }
    /* If client close socket_fd, should return. don't merge for simple reading */
    if (recv_return == 0)
    {
        return nullptr;
    }

    /* read chunk from disk */
    FILE *chunk_fp = fopen(metadata->dst_filename_datanode, "rb");
    if (chunk_fp == NULL)
    {
        printf("[handle_client_read] Failed to open dst_filename_datanode file\n");
        return nullptr;
    }
    char *buffer_chunk = (char *)malloc(sizeof(char) * metadata->chunk_size); // buffer for EC chunk
    if (fread(buffer_chunk, sizeof(char), (size_t)metadata->chunk_size, chunk_fp) != (size_t)metadata->chunk_size)
    {
        printf("[handle_client_read] Failed to read chunk from file\n");
        return nullptr;
    }
    fclose(chunk_fp);

    /* send chunk data to client*/
    if (send(client_fd, buffer_chunk, metadata->chunk_size, 0) < 0)
    {
        printf("[handle_client_read] Failed to send chunk data to client\n");
        return nullptr;
    }
    free(buffer_chunk);
    free(metadata);
    return nullptr;
}

void *client_write_ecx(void *arg)
{
    int tmp_ecm = *((int *)arg);
    printf("[client_write_ecx] Write ecx running %d\n", tmp_ecm);
    int tmp_return;  // return check
    int datanode_fd; // datanode socket
    int client_fd;   // client socket

    /* initialize server network */
    tmp_return = server_initialize_network(&datanode_fd, EC_WRITE_ECX_BASE_PORT + tmp_ecm);
    if (tmp_return == EC_ERROR)
    {
        return nullptr;
    }

    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    pthread_t tid;

    /* wait for client connection */
    while (1)
    {
        /* accept client */
        if ((client_fd = accept(datanode_fd, (struct sockaddr *)&client_addr, &client_addr_len)) == -1)
        {
            printf("[client_write] Failed to accept socket\n");
            return nullptr;
        }

        /* create thread to handle client send */
        if (pthread_create(&tid, NULL, handle_client_write_ecx, (void *)&client_fd) != 0)
        {
            printf("[client_write] Failed to create handle_client_write_ecx thread\n");
            continue;
        }
    }
}

void *client_write_request(void *arg)
{

    printf("[client_write_request] Write request running\n");
    int tmp_return;  // return check
    int datanode_fd; // datanode socket
    int client_fd;   // client socket

    /* initialize server network */
    tmp_return = server_initialize_network(&datanode_fd, EC_WRITE_REQUEST_BASE_PORT + ecm - EC_K);
    if (tmp_return == EC_ERROR)
    {
        return nullptr;
    }

    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    pthread_t tid;

    /* wait for client connection */
    while (1)
    {
        /* accept client */
        if ((client_fd = accept(datanode_fd, (struct sockaddr *)&client_addr, &client_addr_len)) == -1)
        {
            printf("[client_write] Failed to accept socket\n");
            return nullptr;
        }

        /* create thread to handle client send */
        if (pthread_create(&tid, NULL, handle_client_write_request, (void *)&client_fd) != 0)
        {
            printf("[client_write] Failed to create handle_client_write_request thread\n");
            continue;
        }
    }
}

void *client_write_new(void *arg)
{
    int eck = *((int *)arg);
    printf("[client_write_new] Write running %d\n", eck);
    int tmp_return;  // return check
    int datanode_fd; // datanode socket
    int client_fd;   // client socket

    /* initialize server network */
    tmp_return = server_initialize_network(&datanode_fd, EC_WRITE_ECK_BASE_PORT + eck);
    if (tmp_return == EC_ERROR)
    {
        return nullptr;
    }

    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    pthread_t tid;

    /* wait for client connection */
    while (1)
    {
        /* accept client */
        if ((client_fd = accept(datanode_fd, (struct sockaddr *)&client_addr, &client_addr_len)) == -1)
        {
            printf("[client_write_new] Failed to accept socket\n");
            return nullptr;
        }

        /* create thread to handle client send */
        if (pthread_create(&tid, NULL, handle_client_write_new, (void *)&client_fd) != 0)
        {
            printf("[client_write_new] Failed to create handle_client_write thread\n");
            continue;
        }
    }
}

void *client_write(void *arg)
{
    printf("[client_write] Write running\n");
    int tmp_return; // return check

    int datanode_fd; // datanode socket
    int client_fd;   // client socket

    /* initialize server network */
    tmp_return = server_initialize_network(&datanode_fd, EC_WRITE_PORT);
    if (tmp_return == EC_ERROR)
    {
        return nullptr;
    }

    /* Create coding matrix */
    matrix = reed_sol_vandermonde_coding_matrix(EC_K, EC_M, EC_W);

    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    pthread_t tid;

    /* wait for client connection */
    while (1)
    {
        /* accept client */
        if ((client_fd = accept(datanode_fd, (struct sockaddr *)&client_addr, &client_addr_len)) == -1)
        {
            printf("[client_write] Failed to accept socket\n");
            return nullptr;
        }

        /* create thread to handle client send */
        if (pthread_create(&tid, NULL, handle_client_write, (void *)&client_fd) != 0)
        {
            printf("[client_write] Failed to create handle_client_write thread\n");
            continue;
        }
    }
}

void *client_read(void *arg)
{
    printf("[client_read] Read running\n");
    int tmp_return; // return check

    int datanode_fd; // datanode socket
    int client_fd;   // client socket

    /* initialize_network */
    tmp_return = server_initialize_network(&datanode_fd, EC_READ_PORT);
    if (tmp_return == EC_ERROR)
    {
        return nullptr;
    }
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    pthread_t tid;

    /* wait for client connection */
    while (1)
    {
        /* accept client */
        if ((client_fd = accept(datanode_fd, (struct sockaddr *)&client_addr, &client_addr_len)) == -1)
        {
            printf("[client_read] Failed to accept socket\n");
            continue;
        }
        /* create thread to handle client read */
        if (pthread_create(&tid, NULL, handle_client_read, (void *)&client_fd) != 0)
        {
            printf("[client_read] Failed to create thread\n");
            continue;
        }
    }

    close(datanode_fd);
}

int main()
{
    printf("[main] Datanode running...\n");

    pthread_t tid_write, tid_read, tid_write_new[EC_K], tid_write_request, tid_write_ecx[EC_M];

    /* create client_write thread to handle client write */
    if (pthread_create(&tid_write, NULL, client_write, nullptr) != 0)
    {
        printf("[main] Failed to create client_write thread\n");
        return EC_ERROR;
    }

    /* create client_read thread to handle client read */
    if (pthread_create(&tid_read, NULL, client_read, nullptr) != 0)
    {
        printf("[main] Failed to create client_read thread\n");
        return EC_ERROR;
    }

    int i;
    int eck_array[EC_K];
    for (i = 0; i < EC_K; i++)
    {
        eck_array[i] = i;
        /* create client_write_new thread to handle client_write_new */
        if (pthread_create(&tid_write_new[i], NULL, client_write_new, (void *)&eck_array[i]) != 0)
        {
            printf("[main] Failed to create client_write_new thread\n");
            return EC_ERROR;
        }
    }
    int local_ip_last_num;
    int tmp_return;
    tmp_return = get_local_ip_lastnum(&local_ip_last_num);
    if (tmp_return == EC_ERROR)
    {
        printf("[main] Failed to get_local_ip_lastnum\n");
        return EC_ERROR;
    }
    ecm = local_ip_last_num - DATANODE_START_IP_ADDR;
    cur_block_net = ecm - EC_K;
    cur_block_enc = ecm - EC_K;
    cur_block_request_ecx = ecm - EC_K;
    cur_block_request = EC_N;

    int ecm_array[EC_X];
    for (i = 0; i < EC_X; i++)
    {
        ecm_array[i] = i;
        /* create client_write_ecx thread to handle client_write_ecx */
        if (pthread_create(&tid_write_ecx[i], NULL, client_write_ecx, (void *)&ecm_array[i]) != 0)
        {
            printf("[main] Failed to create client_write_ecx thread\n");
            return EC_ERROR;
        }
    }

    /* create client_write thread to handle client write request */
    if (pthread_create(&tid_write_request, NULL, client_write_request, nullptr) != 0)
    {
        printf("[main] Failed to create client write request thread\n");
        return EC_ERROR;
    }

    /* wait until thread end */
    if (pthread_join(tid_write, nullptr) != 0)
    {
        printf("[main] Failed to join client_write thread\n");
        return EC_ERROR;
    }
    if (pthread_join(tid_read, nullptr) != 0)
    {
        printf("[main] Failed to join client_read thread\n");
        return EC_ERROR;
    }

    for (i = 0; i < EC_K; i++)
    {
        if (pthread_join(tid_write_new[i], nullptr) != 0)
        {
            printf("[main] Failed to join client write new thread\n");
            return EC_ERROR;
        }
    }

    if (pthread_join(tid_write_request, nullptr) != 0)
    {
        printf("[main] Failed to join client write request thread\n");
        return EC_ERROR;
    }
    for (i = 0; i < EC_M; i++)
    {
        if (pthread_join(tid_write_ecx[i], nullptr) != 0)
        {
            printf("[main] Failed to join client write ecx thread\n");
            return EC_ERROR;
        }
    }

    return EC_OK;
}