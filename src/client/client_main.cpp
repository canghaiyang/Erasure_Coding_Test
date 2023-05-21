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
#include <iostream>
#include <netdb.h>
#include <ifaddrs.h>

#include "ych_ec_test.h"
#include "jerasure.h"
#include "reed_sol.h"
#include "galois.h"

int erasures[EC_K + EC_M];
double time_enc = 0.0;
double time_net_k = 0.0;
double time_net_m = 0.0;
double time_new_min = 0.0;
double time_new = 0.0;

/*
 * read k+m chunks from file to chunks, if file not enough, padding
 * fp is file pointer of src_filename
 */
int read_file_to_buffer(FILE *src_fp, char *buffer, int buffer_size)
{
    /* Check parameters */
    if (src_fp == NULL || buffer == NULL)
    {
        printf("[read_file_to_buffer] Error parameters: src_fp or buffer\n");
        return EC_ERROR;
    }

    /* Read file to buffer */
    int size_fread;
    size_fread = fread(buffer, sizeof(char), buffer_size, src_fp);

    /* Padding buffer */
    if (size_fread < buffer_size)
    {
        for (int j = size_fread; j < buffer_size; j++)
        {
            buffer[j] = '0';
        }
        return 0; // padding read and IO read
    }
    return 1; //  all IO read
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
        close(server_fd);
        return EC_ERROR;
    }

    /* listen socket */
    if (listen(server_fd, 5) == -1)
    {
        printf("[datanode_initialize network] Failed to listen socket\n");
        close(server_fd);
        return EC_ERROR;
    }
    *server_fd_p = server_fd;
    return EC_OK;
}

void *handle_coding_datanode_write(void *arg)
{
    int datanode_fd = *((int *)arg);
    int chunk_ok = 0;

    /* recv chunk response */
    if (recv(datanode_fd, &chunk_ok, sizeof(chunk_ok), 0) < 0)
    {
        printf("[handle_coding_datanode_write] Failed to recv chunk_ok\n");
        return nullptr;
    }
    int error_response = 1;
    if (send(datanode_fd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_coding_datanode_write] Failed to send response to coding datanode\n");
        return nullptr;
    }
    close(datanode_fd);
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
        printf("[send_one_block_datanode] Failed to recv block metadata response from datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    if (error_response == 0)
    {
        printf("[send_one_block_datanode] Failed to recv block metadata response from datanode\n");
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
    return nullptr;
}

int send_blocks_eck_datanodes(char **block, int chunk_size, int block_size, int remain_block_size, char *dst_filename_stripe, int client_fd)
{
    metadata_t metadata_array[EC_K * EC_N];
    int sockfd_array[EC_K];
    pthread_t tid[EC_K * EC_N];
    int error_flag = EC_OK;
    int cur;
    int i, j;

    int tmp_return;
    for (i = 0; i < EC_K; i++)
    {
        tmp_return = initialize_network(&sockfd_array[i], EC_WRITE_NEW_PORT, i);
        if (tmp_return == EC_ERROR)
        {
            printf("[send_blocks_eck_datanodes] Failed to initialize network\n");
            return EC_ERROR;
        }
    }

    /* Timing variables */
    struct timeval t_new1, t_new2;

    struct timezone tz;
    gettimeofday(&t_new1, &tz);
#if (TEST_LOG)
    struct timeval t_new1_net_1, t_new2_net_1;     // ych_test
    struct timeval t_new1_net_all, t_new2_net_all; // ych_test
    gettimeofday(&t_new1_net_all, &tz);            // ych_test
#endif
    /* Send to each datanode */
    for (j = 0; j < EC_N; j++)
    {
        for (i = 0; i < EC_K; i++)
        {
#if (TEST_LOG)
            gettimeofday(&t_new1_net_1, &tz); // ych_test
#endif
            cur = i * EC_N + j;
            /* Initialize thread metadata */
            metadata_array[cur].sockfd = sockfd_array[i];
            metadata_array[cur].chunk_size = chunk_size;
            if (j == 0)
            {
                metadata_array[cur].block_size = block_size + remain_block_size;
            }
            else
            {
                metadata_array[cur].block_size = block_size;
            }
            metadata_array[cur].remain_block_size = remain_block_size;
            metadata_array[cur].cur_block = j;
            metadata_array[cur].cur_eck = i;
            metadata_array[cur].data = block[cur];
            memset(metadata_array[cur].dst_filename_datanode, 0, sizeof(metadata_array[cur].dst_filename_datanode));
            sprintf(metadata_array[cur].dst_filename_datanode, "%s_%d", dst_filename_stripe, i + 1); // filename on datanode
            metadata_array[cur].error_flag = EC_OK;

            /* Create thread to send one block to one datanode */
            if (pthread_create(&tid[cur], NULL, send_one_block_datanode, (void *)&metadata_array[cur]) != 0)
            {
                printf("[send_blocks_eck_datanodes] Failed to create thread\n");
                return EC_ERROR;
            }

#if (SEND_METHOD)
            /* Wait until thread end */
            if (pthread_join(tid[cur], nullptr) != 0)
            {
                printf("[send_blocks_eck_datanodes] Failed to join thread\n");
                return EC_ERROR;
            }
#if (TEST_LOG)
            gettimeofday(&t_new2_net_1, &tz);
            time_new = 0.0;
            time_new += t_new2_net_1.tv_usec;
            time_new -= t_new1_net_1.tv_usec;
            time_new /= 1000000.0;
            time_new += t_new2_net_1.tv_sec;
            time_new -= t_new1_net_1.tv_sec;
            printf("ych_test 1 round time = %0.10f\n", time_new);
#endif

#endif
        }
    }

#if (!SEND_METHOD)
    for (j = 0; j < EC_N; j++)
    {
        for (i = 0; i < EC_K; i++)
        {
            /* Wait until thread end */
            cur = i * EC_N + j;
            if (pthread_join(tid[cur], nullptr) != 0)
            {
                printf("[send_blocks_eck_datanodes] Failed to join thread\n");
                return EC_ERROR;
            }
        }
    }
#endif

#if (TEST_LOG)
    gettimeofday(&t_new2_net_all, &tz); // ych_test
    time_new = 0.0;
    time_new += t_new2_net_all.tv_usec;
    time_new -= t_new1_net_all.tv_usec;
    time_new /= 1000000.0;
    time_new += t_new2_net_all.tv_sec;
    time_new -= t_new1_net_all.tv_sec;
    printf("ych_test 1 round time = %0.10f\n", time_new);
#endif
    /* check if error */
    for (j = 0; j < EC_N; j++)
    {
        for (i = 0; i < EC_K; i++)
        {
            cur = i * EC_N + j;
            error_flag += metadata_array[cur].error_flag;
        }
    }
    if (error_flag != EC_OK)
    {
        return EC_ERROR;
    }

    int datanode_fd; // datanode socket
    int chunk_count = EC_M;
    struct sockaddr_in datanode_addr;
    socklen_t datanode_addr_len = sizeof(datanode_addr);
    pthread_t tid_client;

    while (chunk_count > 0)
    {
        /* accept client */
        if ((datanode_fd = accept(client_fd, (struct sockaddr *)&datanode_addr, &datanode_addr_len)) == -1)
        {
            printf("[send_blocks_eck_datanodes] Failed to accept socket\n");
            close(client_fd);
            return EC_ERROR;
        }
        /* create thread to handle client send */
        if (pthread_create(&tid_client, NULL, handle_coding_datanode_write, (void *)&datanode_fd) != 0)
        {
            printf("[send_blocks_eck_datanodes] Failed to create write thread\n");
            close(datanode_fd);
            return EC_ERROR;
        }
        /* Wait until thread end */
        if (pthread_join(tid[cur], nullptr) != 0)
        {
            printf("[send_blocks_eck_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
        chunk_count--;
    }

    gettimeofday(&t_new2, &tz);
    time_new = 0.0;
    time_new += t_new2.tv_usec;
    time_new -= t_new1.tv_usec;
    time_new /= 1000000.0;
    time_new += t_new2.tv_sec;
    time_new -= t_new1.tv_sec;
    if (time_new < time_new_min)
    {
        time_new_min = time_new;
    }

    for (i = 0; i < EC_K; i++)
    {
        close(sockfd_array[i]);
    }

    return EC_OK;
}

void *send_one_chunk_datanode(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;

    /* Send chunk metadata and recv response */
    if (send(metadata->sockfd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[send_one_chunk_datanode] Failed to send chunk metadata to datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    int error_response = 0;
    if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[send_one_chunk_datanode] Failed to recv chunk metadata response from datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    if (error_response == 0)
    {
        printf("[send_one_chunk_datanode] Failed to recv chunk metadata response from datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }

    /* Send chunk data and recv response */
    if (send(metadata->sockfd, metadata->data, metadata->chunk_size, 0) < 0)
    {
        printf("[send_one_chunk_datanode] Failed to send chunk data to datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[send_one_chunk_datanode] Failed to recv chunk data response from datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }
    if (error_response == 1)
    {
        printf("[send_one_chunk_datanode] Failed to recv chunk data response from datanode: recv error\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }

    return nullptr;
}

int send_chunks_datanodes(char **data, char **coding, int chunk_size, char *dst_filename_stripe)
{
    metadata_t metadata_array[EC_K + EC_M];
    pthread_t tid[EC_K + EC_M];
    int error_flag = EC_OK;

    /* Send to each datanode */
    for (int i = 0; i < EC_K + EC_M; i++)
    {
        /* Initialize thread metadata */
        int tmp_return = initialize_network(&metadata_array[i].sockfd, EC_WRITE_PORT, i);
        if (tmp_return == EC_ERROR)
        {
            printf("[send_chunks_datanodes] Failed to initialize network\n");
            return EC_ERROR;
        }
        metadata_array[i].chunk_size = chunk_size;
        metadata_array[i].block_size = -1; // Convenient to check if chunk or block
        if (i < EC_K)
        {
            metadata_array[i].data = data[i];
        }
        else
        {
            metadata_array[i].data = coding[i - EC_K];
        }

        memset(metadata_array[i].dst_filename_datanode, 0, sizeof(metadata_array[i].dst_filename_datanode));
        sprintf(metadata_array[i].dst_filename_datanode, "%s_%d", dst_filename_stripe, i + 1); // filename on datanode
        metadata_array[i].error_flag = EC_OK;

        /* Create thread to send one chunk to one datanode */
        if (pthread_create(&tid[i], NULL, send_one_chunk_datanode, (void *)&metadata_array[i]) != 0)
        {
            printf("[send_chunks_datanodes] Failed to create thread\n");
            return EC_ERROR;
        }

#if (SEND_METHOD)
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[send_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
        close(metadata_array[i].sockfd);

#endif
    }
#if (!SEND_METHOD)
    for (int i = 0; i < EC_K + EC_M; i++)
    {
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[send_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
        close(metadata_array[i].sockfd);
    }
#endif
    /* check if error */
    for (int i = 0; i < EC_K + EC_M; i++)
    {
        error_flag += metadata_array[i].error_flag;
    }
    if (error_flag != EC_OK)
    {
        return EC_ERROR;
    }
    return EC_OK;
}

int send_chunks_datanodes_k(char **data, int chunk_size, char *dst_filename_stripe)
{
    metadata_t metadata_array[EC_K];
    pthread_t tid[EC_K];
    int error_flag = EC_OK;
    int i;
    int tmp_return;

    for (i = 0; i < EC_K; i++)
    {
        tmp_return = initialize_network(&metadata_array[i].sockfd, EC_WRITE_PORT, i);
        if (tmp_return == EC_ERROR)
        {
            printf("[send_chunks_datanodes_k] Failed to initialize network\n");
            return EC_ERROR;
        }
    }

    /* Timing variables */
    struct timeval t_net1, t_net2;
    struct timezone tz;
    double tsec;
    gettimeofday(&t_net1, &tz);

    /* Send to each datanode */
    for (i = 0; i < EC_K; i++)
    {
        /* Initialize thread metadata */
        metadata_array[i].chunk_size = chunk_size;
        metadata_array[i].block_size = -1; // Convenient to check if chunk or block
        metadata_array[i].cur_eck = -3;    // Convenient to check if chunk or block
        metadata_array[i].data = data[i];
        memset(metadata_array[i].dst_filename_datanode, 0, sizeof(metadata_array[i].dst_filename_datanode));
        sprintf(metadata_array[i].dst_filename_datanode, "%s_%d", dst_filename_stripe, i + 1); // filename on datanode
        metadata_array[i].error_flag = EC_OK;

        /* Create thread to send one chunk to one datanode */
        if (pthread_create(&tid[i], NULL, send_one_chunk_datanode, (void *)&metadata_array[i]) != 0)
        {
            printf("[send_chunks_datanodes_k] Failed to create thread\n");
            return EC_ERROR;
        }

#if (SEND_METHOD)
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[send_chunks_datanodes_k] Failed to join thread\n");
            return EC_ERROR;
        }

#endif
    }
#if (!SEND_METHOD)
    for (int i = 0; i < EC_K + EC_M; i++)
    {
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[send_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
    }
#endif

    gettimeofday(&t_net2, &tz);
    tsec = 0.0;
    tsec += t_net2.tv_usec;
    tsec -= t_net1.tv_usec;
    tsec /= 1000000.0;
    tsec += t_net2.tv_sec;
    tsec -= t_net1.tv_sec;
    time_net_k = tsec; // Easy to sum net_k time

    /* check if error */
    for (int i = 0; i < EC_K; i++)
    {
        error_flag += metadata_array[i].error_flag;
    }
    if (error_flag != EC_OK)
    {
        return EC_ERROR;
    }
    for (i = 0; i < EC_K; i++)
    {
        close(metadata_array[i].sockfd);
    }

    return EC_OK;
}

int send_chunks_datanodes_m(char **coding, int chunk_size, char *dst_filename_stripe)
{
    metadata_t metadata_array[EC_K + EC_M];
    pthread_t tid[EC_K + EC_M];
    int error_flag = EC_OK;
    int i;
    int tmp_return;

    for (i = EC_K; i < EC_K + EC_M; i++)
    {
        tmp_return = initialize_network(&metadata_array[i].sockfd, EC_WRITE_PORT, i);
        if (tmp_return == EC_ERROR)
        {
            printf("[send_chunks_datanodes_m] Failed to initialize network\n");
            return EC_ERROR;
        }
    }

    /* Timing variables */
    struct timeval t_net1, t_net2;
    struct timezone tz;
    double tsec;
    gettimeofday(&t_net1, &tz);

    /* Send to each datanode */
    for (i = EC_K; i < EC_K + EC_M; i++)
    {
        metadata_array[i].chunk_size = chunk_size;
        metadata_array[i].block_size = -1; // Convenient to check if chunk or block
        metadata_array[i].data = coding[i - EC_K];
        memset(metadata_array[i].dst_filename_datanode, 0, sizeof(metadata_array[i].dst_filename_datanode));
        sprintf(metadata_array[i].dst_filename_datanode, "%s_%d", dst_filename_stripe, i + 1); // filename on datanode
        metadata_array[i].error_flag = EC_OK;

        /* Create thread to send one chunk to one datanode */
        if (pthread_create(&tid[i], NULL, send_one_chunk_datanode, (void *)&metadata_array[i]) != 0)
        {
            printf("[send_chunks_datanodes_m] Failed to create thread\n");
            return EC_ERROR;
        }

#if (SEND_METHOD)
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[send_chunks_datanodes_m] Failed to join thread\n");
            return EC_ERROR;
        }

#endif
    }
#if (!SEND_METHOD)
    for (i = 0; i < EC_K + EC_M; i++)
    {
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[send_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
    }
#endif

    gettimeofday(&t_net2, &tz);
    tsec = 0.0;
    tsec += t_net2.tv_usec;
    tsec -= t_net1.tv_usec;
    tsec /= 1000000.0;
    tsec += t_net2.tv_sec;
    tsec -= t_net1.tv_sec;
    time_net_m = tsec; // Easy to sum net_k time

    /* check if error */
    for (i = EC_K; i < EC_K + EC_M; i++)
    {
        error_flag += metadata_array[i].error_flag;
    }
    if (error_flag != EC_OK)
    {
        return EC_ERROR;
    }
    for (i = EC_K; i < EC_K + EC_M; i++)
    {
        close(metadata_array[i].sockfd);
    }

    return EC_OK;
}

void *recv_one_chunk_datanode(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;

    /* Send chunk metadata */
    if (send(metadata->sockfd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[recv_one_chunk_datanode] Failed to send chunk metadata to datanode\n");
        metadata->error_flag = EC_ERROR;
        return nullptr;
    }

    /*Recv data chunk */
    int recv_size;
    int tmp_chunk_size = metadata->chunk_size;
    char *tmp_buffer_chunk = metadata->data;
    while (tmp_chunk_size > 0)
    {
        recv_size = recv(metadata->sockfd, tmp_buffer_chunk, tmp_chunk_size, 0);
        if (recv_size < 0)
        {
            printf("[recv_one_chunk_datanode] Failed to recv chunc data\n");
            metadata->error_flag = EC_ERROR;
            return nullptr;
        }
        tmp_chunk_size -= recv_size;
        tmp_buffer_chunk += recv_size;
    }
    return nullptr;
}

int recv_data_chunks_datanodes(int *num_data_chunk_p, char **data, int chunk_size, char *dst_filename_stripe)
{
    metadata_t metadata_array[EC_K + EC_M];
    pthread_t tid[EC_K + EC_M];
    int error_flag = EC_OK;
    int num_data_chunk = *num_data_chunk_p;

    /* Recv to k datanode that saved data chunk */
    for (int i = 0; i < EC_K; i++)
    {
        /* Initialize thread metadata */
        int tmp_return = initialize_network(&metadata_array[i].sockfd, EC_READ_PORT, i);
        if (tmp_return == EC_ERROR)
        {
            erasures[num_data_chunk] = i;
            num_data_chunk++;
            printf("[recv_data_chunks_datanodes] Failed to initialize network\n");
            /* skip lost datanode */
            metadata_array[i].sockfd = -1;
            metadata_array[i].error_flag = EC_OK;
            continue;
        }
        metadata_array[i].chunk_size = chunk_size;
        metadata_array[i].data = data[i];
        memset(metadata_array[i].dst_filename_datanode, 0, sizeof(metadata_array[i].dst_filename_datanode));
        sprintf(metadata_array[i].dst_filename_datanode, "%s_%d", dst_filename_stripe, i + 1); // filename on datanode
        metadata_array[i].error_flag = EC_OK;

        /* Create thread to send one chunk to one datanode */
        if (pthread_create(&tid[i], NULL, recv_one_chunk_datanode, (void *)&metadata_array[i]) != 0)
        {
            printf("[recv_data_chunks_datanodes] Failed to create thread\n");
            return EC_ERROR;
        }

#if (RECV_METHOD)
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[recv_data_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
        close(metadata_array[i].sockfd);
#endif
    }
#if (!RECV_METHOD)
    for (int i = 0; i < EC_K; i++)
    {
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[recv_data_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
        if (metadata_array[i].sockfd != -1)
        {
            close(metadata_array[i].sockfd);
        }
    }
#endif
    /* check if error */
    for (int i = 0; i < EC_K; i++)
    {
        error_flag += metadata_array[i].error_flag;
    }
    if (error_flag != EC_OK)
    {
        return EC_ERROR;
    }
    *num_data_chunk_p = num_data_chunk;

    return EC_OK;
}

int recv_coding_chunks_datanodes(int *num_data_chunk_p, char **coding, int num_need_coding, int chunk_size, char *dst_filename_stripe)
{

    metadata_t metadata_array[EC_K + EC_M];
    pthread_t tid[EC_K + EC_M];
    int tmp_num = num_need_coding;
    int error_flag = EC_OK;
    int num_data_chunk = *num_data_chunk_p;

    /* Recv to m datanode that saved coding chunk */
    for (int i = EC_K; i < EC_K + EC_M && tmp_num > 0; i++)
    {
        tmp_num--;

        /* Initialize thread metadata */
        int tmp_return = initialize_network(&metadata_array[i].sockfd, EC_READ_PORT, i);
        if (tmp_return == EC_ERROR)
        {
            erasures[num_data_chunk] = i;
            num_data_chunk++;
            printf("[recv_coding_chunks_datanodes] Failed to initialize network\n");
            /* skip lost datanode */
            metadata_array[i].sockfd = -1;
            continue;
        }
        metadata_array[i].chunk_size = chunk_size;
        metadata_array[i].data = coding[i - EC_K];
        memset(metadata_array[i].dst_filename_datanode, 0, sizeof(metadata_array[i].dst_filename_datanode));
        sprintf(metadata_array[i].dst_filename_datanode, "%s_%d", dst_filename_stripe, i + 1); // filename on datanode
        metadata_array[i].error_flag = EC_OK;

        /* Create thread to send one chunk to one datanode */
        if (pthread_create(&tid[i], NULL, recv_one_chunk_datanode, (void *)&metadata_array[i]) != 0)
        {
            printf("[recv_coding_chunks_datanodes] Failed to create thread\n");
            return EC_ERROR;
        }

#if (RECV_METHOD)
        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[recv_coding_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
        close(metadata_array[i].sockfd);
#endif
    }
#if (!RECV_METHOD)
    for (int i = EC_K; i < EC_K + EC_M && num_need_coding > 0; i++)
    {
        /* skip lost datanode */
        if (client_sockfd[i] == -1)
        {
            continue;
        }
        num_need_coding--;

        /* Wait until thread end */
        if (pthread_join(tid[i], nullptr) != 0)
        {
            printf("[recv_chunks_datanodes] Failed to join thread\n");
            return EC_ERROR;
        }
        if (metadata_array[i].sockfd != -1)
        {
            close(metadata_array[i].sockfd);
        }
    }
#endif
    /* check if error */
    for (int i = EC_K; i < EC_K + EC_M && num_need_coding > 0; i++)
    {
        error_flag += metadata_array[i].error_flag;
        num_need_coding--;
    }
    if (error_flag != EC_OK)
    {
        return EC_ERROR;
    }
    *num_data_chunk_p = num_data_chunk;
    return EC_OK;
}

void *encode_thread(void *arg)
{
    encode_t *encode_metadata = (encode_t *)arg;

    /* Timing variables */
    struct timeval t_enc1, t_enc2;
    struct timezone tz;
    double tsec;

    gettimeofday(&t_enc1, &tz);

    /* Encode */
    jerasure_matrix_encode(EC_K, EC_M, EC_W, encode_metadata->matrix, encode_metadata->data, encode_metadata->coding, encode_metadata->chunk_size);

    gettimeofday(&t_enc2, &tz);
    tsec = 0.0;
    tsec += t_enc2.tv_usec;
    tsec -= t_enc1.tv_usec;
    tsec /= 1000000.0;
    tsec += t_enc2.tv_sec;
    tsec -= t_enc1.tv_sec;
    time_enc = tsec; // Easy to sum enc time

    return nullptr;
}

void *encode_mul_thread(void *arg)
{
    encode_t *encode_metadata = (encode_t *)arg;
    encode_t *mul_encode_metadata = (encode_t *)malloc(sizeof(encode_t) * ENC_THREAD_NUM);

    /* Timing variables */
    struct timeval t_enc1, t_enc2;
    struct timezone tz;
    double tsec;

    int thread_num;
    int cur_size = 0;
    int i;
    // char **t_data = (char **)malloc(sizeof(char *) * EC_K); // thread data chunk s
    // char **t_coding = (char **)malloc(sizeof(char *) * EC_M); // thread coding chunks

    char ***t_data_array = (char ***)malloc(sizeof(char **) * ENC_THREAD_NUM);
    char ***t_coding_array = (char ***)malloc(sizeof(char **) * ENC_THREAD_NUM);
    for (thread_num = 0; thread_num < ENC_THREAD_NUM; thread_num++)
    {
        t_data_array[thread_num] = (char **)malloc(sizeof(char *) * EC_K);
        t_coding_array[thread_num] = (char **)malloc(sizeof(char *) * EC_K);
    }

    pthread_t *p_tid_encode = (pthread_t *)malloc(sizeof(pthread_t) * ENC_THREAD_NUM);

    gettimeofday(&t_enc1, &tz);

    for (thread_num = 0; thread_num < ENC_THREAD_NUM; thread_num++) // encoding multiple thread
    {
        int t_chunk_size; // thread chunk size
        if (thread_num == 0)
        {
            t_chunk_size = encode_metadata->chunk_size / ENC_THREAD_NUM + encode_metadata->chunk_size % ENC_THREAD_NUM;
        }
        else
        {
            t_chunk_size = encode_metadata->chunk_size / ENC_THREAD_NUM;
        }

        for (i = 0; i < EC_K; i++) /* Set pointers to point to file data */
        {
            t_data_array[thread_num][i] = encode_metadata->data[i] + cur_size;
        }
        for (i = 0; i < EC_M; i++)
        {
            t_coding_array[thread_num][i] = encode_metadata->coding[i] + cur_size;
        }

        mul_encode_metadata[thread_num].matrix = encode_metadata->matrix;
        mul_encode_metadata[thread_num].data = t_data_array[thread_num];
        mul_encode_metadata[thread_num].coding = t_coding_array[thread_num];
        mul_encode_metadata[thread_num].chunk_size = t_chunk_size;
        cur_size += t_chunk_size;

        if (pthread_create(&p_tid_encode[thread_num], NULL, encode_thread, (void *)&mul_encode_metadata[thread_num]) != 0) /* Create encode_thread thread to encode */
        {
            printf("[encode_mul_thread] Failed to create encode_thread thread\n");
            return nullptr;
        }
    }

    for (thread_num = 0; thread_num < ENC_THREAD_NUM; thread_num++) // encoding multiple thread
    {
        if (pthread_join(p_tid_encode[thread_num], nullptr) != 0) /* Wait until encode_thread thread end */
        {
            printf("[encode_mul_thread] Failed to join encode_thread thread\n");
            return nullptr;
        }
    }

    gettimeofday(&t_enc2, &tz);
    tsec = 0.0;
    tsec += t_enc2.tv_usec;
    tsec -= t_enc1.tv_usec;
    tsec /= 1000000.0;
    tsec += t_enc2.tv_sec;
    tsec -= t_enc1.tv_sec;
    time_enc = tsec; // Easy to sum enc time

    for (thread_num = 0; thread_num < ENC_THREAD_NUM; thread_num++)
    {
        free(t_data_array[thread_num]);
        free(t_coding_array[thread_num]);
    }
    free(t_data_array);
    free(t_coding_array);
    free(mul_encode_metadata);
    free(p_tid_encode);
    return nullptr;
}

void *network_k_thread(void *arg)
{
    network_t *network_metadata = (network_t *)arg;
    int tmp_return;

    /* send chunks to each datanode */
    tmp_return = send_chunks_datanodes_k(network_metadata->data, network_metadata->chunk_size, network_metadata->dst_filename_stripe);
    if (tmp_return == EC_ERROR)
    {
        printf("[network_k_thread] Failed to send k chunks to datanode");
        return nullptr;
    }

    return nullptr;
}

void *network_m_thread(void *arg)
{
    network_t *network_metadata = (network_t *)arg;
    int tmp_return;

    /* send chunks to each datanode */
    tmp_return = send_chunks_datanodes_m(network_metadata->coding, network_metadata->chunk_size, network_metadata->dst_filename_stripe);
    if (tmp_return == EC_ERROR)
    {
        printf("[network_k_thread] Failed to send m chunks to datanode");
        return nullptr;
    }
    return nullptr;
}

static void help(int argc, char **argv)
{
    std::cerr << "Usage: " << argv[0] << " cmd...\n"
              << "\t -h \n"
              << "\t -w <src_filename> <dst_filename> \n"
              << "\t -r <src_filename> <dst_filename> \n"
              << "\t -kw <src_filename> <dst_filename> \n"
              << "\t  \n"
              << "\t Example1: -w src_10MB.mp4 dst_10MB.mp4 \n"
              << "\t <src_filename> saved on ych_ec_test/test_file/write/ for client\n"
              << "\t <dst_filename> saved on ych_ec_test/test_file/write/ for datanode \n"
              << "\t Example2: -r read_10MB.mp4 dst_10MB.mp4  \n"
              << "\t <src_filename> saved on ych_ec_test/test_file/read/ for datanode\n"
              << "\t <dst_filename> saved on ych_ec_test/test_file/write/ for client \n"
              << "\t Tip: saved filename on datanode actually is dst_filenameX_Y, \n"
              << "\t the X is Xth stripe and the Y is Yth chunk.\n"
              << "\t  \n"
              << std::endl;
}

static int erasure_coding_write_eck(int argc, char **argv)
{
    printf("[erasure_coding_write_eck] Write fast running...\n");
#if (SEND_DATANODE)
    int i, j;       // loop control variables
    int tmp_return; // return check

    /* File arguments */
    FILE *src_fp;            // src_file pointers
    int file_size;           // size of file
    struct stat file_status; // finding file size
    int reading;             // number of reading required
    int current_reading;     // number of current reading
    int io_flag;             // 1, all IO read; 0, padding read and IO read
    char src_filename[MAX_PATH_LEN] = {0};
    char dst_filename[MAX_PATH_LEN] = {0};
    char file_size_filename[MAX_PATH_LEN] = {0};
    char buf_filename1[MAX_PATH_LEN] = {0};
    char buf_filename2[MAX_PATH_LEN] = {0};
    getcwd(buf_filename1, sizeof(buf_filename1));
    strncpy(buf_filename2, buf_filename1, strlen(buf_filename1) - 6);              // -6 to sub script/
    sprintf(src_filename, "%s%s%s", buf_filename2, WRITE_PATH, argv[2]);           // get src_filename
    sprintf(dst_filename, "%s%s%s", buf_filename2, WRITE_PATH, argv[3]);           // get dst_filename
    sprintf(file_size_filename, "%s%s%s", buf_filename2, FILE_SIZE_PATH, argv[3]); // get file_size_filename to save file size of src file
    printf("[erasure_coding_write_eck] The src_filename = %s\n", src_filename);
    printf("[erasure_coding_write_eck] The dst_filename = %s\n", dst_filename);
    printf("[erasure_coding_write_eck] The file_size_filename = %s\n", file_size_filename);

    /* EC arguments */
    int chunk_size = CHUNK_SIZE * 1024 * 1024;                        // unit Byte
    int block_size = ((chunk_size / (EC_W / 8)) / EC_N) * (EC_W / 8); // block size that need involve finite field
    int remain_block_size = ((chunk_size / (EC_W / 8)) % EC_N) * (EC_W / 8);
    int buffer_size = EC_K * chunk_size;                       // stripe size
    char *buffer = (char *)malloc(sizeof(char) * buffer_size); // buffer for EC stripe
    if (buffer == NULL)
    {
        printf("[erasure_coding_write_eck] Failed to malloc buffer: buffer_size = %d Bytes\n", buffer_size);
        return EC_ERROR;
    }

    char **data = (char **)malloc(sizeof(char *) * EC_K);         // data chunk
    char **block = (char **)malloc(sizeof(char *) * EC_K * EC_N); // data block

    /* Set pointers to point to file data */
    for (i = 0; i < EC_K; i++)
    {
        data[i] = buffer + (i * chunk_size);
        for (j = 0; j < EC_N; j++)
        {
            if (j == 0)
            {
                block[i * EC_N + j] = data[i];
            }
            else
            {
                block[i * EC_N + j] = data[i] + remain_block_size + j * block_size;
            }
        }
    }

    /* Timing variables */
    double totalsec = 0.0;

    /* Open src_filename and error check */
    src_fp = fopen(src_filename, "rb");
    if (src_fp == NULL)
    {
        printf("[erasure_coding_write_eck] Failed to open file\n");
        return EC_ERROR;
    }

    /* Determine original size of file */
    stat(src_filename, &file_status);
    file_size = file_status.st_size;

    /* Number of reading */
    if (file_size % buffer_size == 0)
    {
        reading = file_size / buffer_size;
    }
    else
    {
        reading = file_size / buffer_size + 1;
    }

    /* For Test */
    if (reading != 1)
    {
        printf("[erasure_coding_write] File size is not %d MB, please use correct file\n", EC_K * CHUNK_SIZE);
        return EC_ERROR;
    }
    int test_n = TEST_N;

    current_reading = 1;

    /* Wait chunk_ok response from coding datanode */
    int client_fd; // client socket
    tmp_return = server_initialize_network(&client_fd, EC_WRITE_PORT);
    if (tmp_return == EC_ERROR)
    {
        printf("[erasure_coding_write_eck] Failed to server_initialize_network\n");
        return EC_ERROR;
    }

    /* Begin EC */
    while (current_reading <= reading)
    {
        /*One IO read file to buffer*/
        io_flag = read_file_to_buffer(src_fp, buffer, buffer_size);
        if (io_flag == EC_ERROR)
        {
            printf("[erasure_coding_write_eck] One IO read file to buffer: current reading = %d\n", current_reading);
            fclose(src_fp);
            return EC_ERROR;
        }
        /* create filename on stripe */
        char dst_filename_stripe[MAX_PATH_LEN] = {0};
        sprintf(dst_filename_stripe, "%s%d", dst_filename, current_reading);

        /* Begin Test */
        int test_times;
        time_new_min = 99999.99999;
        for (test_times = 1; test_times <= test_n; test_times++)
        {
            /* Send blocks to each datanode */
            tmp_return = send_blocks_eck_datanodes(block, chunk_size, block_size, remain_block_size, dst_filename_stripe, client_fd);
            if (tmp_return == EC_ERROR)
            {
                printf("[erasure_coding_write_eck] Failed to send chunks to datanode: current_reading = %d\n", current_reading);
                fclose(src_fp);
                return EC_ERROR;
            }
            totalsec += time_new;
            printf("[erasure_coding_write_eck] Current test times = %d : EC write time (enc+net+disk) = %0.10f\n", test_times, time_new);
        }
        current_reading++;
    }
#if (TEST_LOG)
    printf("[erasure_coding_write_eck] Average EC write time (enc+net+disk) = %0.10f\n", totalsec / reading / test_n);
#endif
    printf("[erasure_coding_write_eck] Min EC write time (enc+net+disk) = %0.10f\n", time_new_min);
    fclose(src_fp);
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);

    /* Write file size to file_size filename */
    FILE *file_size_fp;
    char file_size_buffer[MAX_PATH_LEN] = {0};
    file_size_fp = fopen(file_size_filename, "wb");
    if (file_size_fp == NULL)
    {
        printf("[erasure_coding_write_eck] Failed to open file\n");

        return EC_ERROR;
    }
    sprintf(file_size_buffer, "%d", file_size);
    if (fwrite(file_size_buffer, sizeof(char), sizeof(file_size_buffer), file_size_fp) != sizeof(file_size_buffer))
    {
        printf("[erasure_coding_write_eck] Failed to write file_size file\n");
        fclose(file_size_fp);
        return EC_ERROR;
    }
    fclose(file_size_fp);
    return EC_OK;
#else
    printf("[erasure_coding_write_fast] Network module not opened, because ych_ec_test.h: SEND_DATANODE is 0\n");
    return EC_OK;
#endif
}

static int erasure_coding_write(int argc, char **argv)
{
    printf("[erasure_coding_write] Write running...\n");
    int i;          // loop control variables
    int tmp_return; // return check

    /* File arguments */
    FILE *src_fp;            // src_file pointers
    int file_size;           // size of file
    struct stat file_status; // finding file size
    int reading;             // number of reading required
    int current_reading;     // number of current reading
    int io_flag;             // 1, all IO read; 0, padding read and IO read
    char src_filename[MAX_PATH_LEN] = {0};
    char dst_filename[MAX_PATH_LEN] = {0};
    char file_size_filename[MAX_PATH_LEN] = {0};
    char test_write_io_filename[MAX_PATH_LEN] = {0};
    char buf_filename1[MAX_PATH_LEN] = {0};
    char buf_filename2[MAX_PATH_LEN] = {0};
    getcwd(buf_filename1, sizeof(buf_filename1));
    strncpy(buf_filename2, buf_filename1, strlen(buf_filename1) - 6);              // -6 to sub script/
    sprintf(src_filename, "%s%s%s", buf_filename2, WRITE_PATH, argv[2]);           // get src_filename
    sprintf(dst_filename, "%s%s%s", buf_filename2, WRITE_PATH, argv[3]);           // get dst_filename
    sprintf(file_size_filename, "%s%s%s", buf_filename2, FILE_SIZE_PATH, argv[3]); // get file_size_filename to save file size of src file
    printf("[erasure_coding_write] The src_filename = %s\n", src_filename);
    printf("[erasure_coding_write] The dst_filename = %s\n", dst_filename);
    printf("[erasure_coding_write] The file_size_filename = %s\n", file_size_filename);

    /* EC arguments */
    int chunk_size = CHUNK_SIZE * 1024 * 1024;                 // unit Byte
    int buffer_size = EC_K * chunk_size;                       // stripe size
    char *buffer = (char *)malloc(sizeof(char) * buffer_size); // buffer for EC stripe
    if (buffer == NULL)
    {
        printf("[erasure_coding_write] Failed to malloc buffer: buffer_size = %d Bytes\n", buffer_size);
        return EC_ERROR;
    }

    /* Jerasure arguments */
    int *matrix = NULL;                                     // coding matrix
    char **data = (char **)malloc(sizeof(char *) * EC_K);   // data chunk s
    char **coding = (char **)malloc(sizeof(char *) * EC_M); // coding chunks

    for (i = 0; i < EC_M; i++)
    {
        coding[i] = (char *)malloc(sizeof(char) * chunk_size);
        if (coding[i] == NULL)
        {
            printf("[erasure_coding_write] Failed to malloc coding: current coding_num = %d \n", i);
            return EC_ERROR;
        }
    }

    /* Set pointers to point to file data */
    for (i = 0; i < EC_K; i++)
    {
        data[i] = buffer + (i * chunk_size);
    }

    /* Timing variables */
    struct timeval t_io1, t_io2, t_enc1, t_enc2, t_net1, t_net2;
    struct timezone tz;
    double tsec;
    double max_io = 0.0, totalsec_enc = 0.0, totalsec_net_k = 0.0, totalsec_net_m = 0.0, totalsec_net = 0.0;
    double min_io = 99999.99999, min_enc = 99999.99999, min_net_k = 99999.99999, min_net_m = 99999.99999, min_net = 99999.99999;
    int totalsec_io_count = 0;

    /* Open src_filename and error check */
    src_fp = fopen(src_filename, "rb");
    if (src_fp == NULL)
    {
        printf("[erasure_coding_write] Failed to open file\n");
        return EC_ERROR;
    }

    /* Determine original size of file */
    stat(src_filename, &file_status);
    file_size = file_status.st_size;

    /* Create coding matrix */
    matrix = reed_sol_vandermonde_coding_matrix(EC_K, EC_M, EC_W);

    /* Number of reading */
    if (file_size % buffer_size == 0)
    {
        reading = file_size / buffer_size;
    }
    else
    {
        reading = file_size / buffer_size + 1;
    }

    /* For Test */
    if (reading != 1)
    {
        printf("[erasure_coding_write] File size is not %d MB, please use correct file\n", EC_K * CHUNK_SIZE);
        return EC_ERROR;
    }
    int test_n = TEST_N;
    encode_t *encode_metadata = (encode_t *)malloc(sizeof(encode_t));
    network_t *network_metadata = (network_t *)malloc(sizeof(network_t));

    current_reading = 1;
    /* Begin EC */
    while (current_reading <= reading)
    {

        /*One IO read file to buffer*/
        io_flag = read_file_to_buffer(src_fp, buffer, buffer_size);
        if (io_flag == EC_ERROR)
        {
            printf("[erasure_coding_write] One IO read file to buffer: current reading = %d\n", current_reading);
            fclose(src_fp);
            return EC_ERROR;
        }
        if (io_flag == 0)
        {
            printf("[erasure_coding_write] Read error. File size is not %d MB, please use correct file\n", EC_K * CHUNK_SIZE);
            fclose(src_fp);
            return EC_ERROR;
        }

        /* Begin Test */
        int test_times;
        for (test_times = 1; test_times <= test_n; test_times++)
        {
            printf("[erasure_coding_write] Current test times = %d\n", test_times);

#if (SEND_DATANODE)
            /* Send k chunk */
            char dst_filename_stripe[MAX_PATH_LEN] = {0};
            sprintf(dst_filename_stripe, "%s%d", dst_filename, current_reading); /* create filename on stripe */
            network_metadata->data = data;
            network_metadata->coding = coding;
            network_metadata->chunk_size = chunk_size;
            network_metadata->dst_filename_stripe = dst_filename_stripe;
            pthread_t tid_network_k;
            if (pthread_create(&tid_network_k, NULL, network_k_thread, (void *)network_metadata) != 0) /* Create network_k_thread thread to encode */
            {
                printf("[erasure_coding_write] Failed to create network_k_thread thread\n");
                return EC_ERROR;
            }
#endif
            /* Encode */
            encode_metadata->matrix = matrix;
            encode_metadata->data = data;
            encode_metadata->coding = coding;
            encode_metadata->chunk_size = chunk_size;
            pthread_t tid_encode;
            if (ENC_THREAD_NUM == 1)
            {
                if (pthread_create(&tid_encode, NULL, encode_thread, (void *)encode_metadata) != 0) /* Create encode_thread thread to encode */
                {
                    printf("[erasure_coding_write] Failed to create encode_thread thread\n");
                    return EC_ERROR;
                }
            }
            else
            {
                if (pthread_create(&tid_encode, NULL, encode_mul_thread, (void *)encode_metadata) != 0) /* Create encode_thread thread to encode */
                {
                    printf("[erasure_coding_write] Failed to create encode_thread thread\n");
                    return EC_ERROR;
                }
            }
            if (pthread_join(tid_encode, nullptr) != 0) /* Wait until encode_thread thread end */
            {
                printf("[erasure_coding_write] Failed to join encode_thread thread\n");
                return EC_ERROR;
            }
            totalsec_enc += time_enc;
            if (time_enc < min_enc)
            {
                min_enc = time_enc;
            }
            printf("[encode_thread] Encoding time = %0.10f\n", time_enc);

#if (SEND_DATANODE)
            if (pthread_join(tid_network_k, nullptr) != 0) /* Wait until network_k_thread thread end */
            {
                printf("[erasure_coding_write] Failed to join network_k_thread thread\n");
                return EC_ERROR;
            }
            totalsec_net_k += time_net_k;
            if (time_net_k < min_net_k)
            {
                min_net_k = time_net_k;
            }
            printf("[network_k_thread] Network k chunks time = %0.10f\n", time_net_k);

            /* Send m chunk */
            pthread_t tid_network_m;
            if (pthread_create(&tid_network_m, NULL, network_m_thread, (void *)network_metadata) != 0) /* Create network_m_thread thread to encode */
            {
                printf("[erasure_coding_write] Failed to create network_m_thread thread\n");
                return EC_ERROR;
            }
            if (pthread_join(tid_network_m, nullptr) != 0) /* Wait until network_m_thread thread end */
            {
                printf("[erasure_coding_write] Failed to join network_m_thread thread\n");
                return EC_ERROR;
            }
            totalsec_net_m += time_net_m;
            if (time_net_m < min_net_m)
            {
                min_net_m = time_net_m;
            }
            printf("[network_k_thread] Network m chunks time = %0.10f\n", time_net_m);
#endif
            /* IO write chunk test */
            char test_write_io_filename[MAX_PATH_LEN] = {0};
            sprintf(test_write_io_filename, "%s%s%s", buf_filename2, TEST_WRITE_IO_PATH, "test_io_test");

            FILE *file_test_fp;
            double max_tsec = 0.0;
            for (i = 0; i < EC_K + EC_M; i++)
            {
                gettimeofday(&t_io1, &tz);

                file_test_fp = fopen(test_write_io_filename, "wb");
                if (file_test_fp == NULL)
                {
                    printf("[erasure_coding_write] Failed to open file_test_fp  file\n");
                    return EC_ERROR;
                }

                if (i < EC_K)
                {
                    fwrite(data[i], sizeof(char), (size_t)chunk_size, file_test_fp);
                }
                else
                {
                    fwrite(coding[i - EC_K], sizeof(char), (size_t)chunk_size, file_test_fp);
                }
                fflush(file_test_fp);
                fsync(fileno(file_test_fp));
                fclose(file_test_fp);

                gettimeofday(&t_io2, &tz);
                tsec = 0.0;
                tsec += t_io2.tv_usec;
                tsec -= t_io1.tv_usec;
                tsec /= 1000000.0;
                tsec += t_io2.tv_sec;
                tsec -= t_io1.tv_sec;
                if (tsec > max_tsec)
                {
                    max_tsec = tsec;
                }
                printf("[erasure_coding_write] The %dth chunk: write IO time = %0.10f\n", i + 1, tsec);
            }
#if (TEST_LOG)
            printf("[erasure_coding_write] Max write disk IO time = %0.10f\n", max_tsec);
#endif
            if (max_tsec > max_io)
            {
                max_io = max_tsec;
            }
            if (max_tsec < min_io)
            {
                min_io = max_tsec;
            }
        }
        current_reading++;
    }

    /* write file size to file_size filename as DFS file metadata */
    FILE *file_size_fp;
    char file_size_buffer[MAX_PATH_LEN] = {0};
    file_size_fp = fopen(file_size_filename, "wb");
    if (file_size_fp == NULL)
    {
        printf("[erasure_coding_write] Failed to open file\n");

        return EC_ERROR;
    }
    sprintf(file_size_buffer, "%d", file_size);
    if (fwrite(file_size_buffer, sizeof(char), sizeof(file_size_buffer), file_size_fp) != sizeof(file_size_buffer))
    {
        printf("[erasure_coding_write] Failed to write file_size file\n");
        fclose(file_size_fp);
        return EC_ERROR;
    }
    fclose(file_size_fp);
    totalsec_net = totalsec_net_k + totalsec_net_m;
    min_net = min_net_k + min_net_m;
#if (TEST_LOG)
    printf("[erasure_coding_write] Average network time k chunks = %0.10f\n", totalsec_net_k / reading / test_n);
    printf("[erasure_coding_write] Average network time m chunks = %0.10f\n", totalsec_net_m / reading / test_n);
    printf("[erasure_coding_write] Average network time k+m chunks= %0.10f\n", totalsec_net / reading / test_n);
    printf("[erasure_coding_write] Average encoding time = %0.10f\n", totalsec_enc / reading / test_n);
    printf("[erasure_coding_write] Max write disk IO time = %0.10f\n", max_io);
#endif
    printf("[erasure_coding_write] Min network time k chunks = %0.10f\n", min_net_k);
    printf("[erasure_coding_write] Min network time m chunks = %0.10f\n", min_net_m);
    printf("[erasure_coding_write] Min network time k+m chunks= %0.10f\n", min_net);
    printf("[erasure_coding_write] Min encoding time = %0.10f\n", min_enc);
    printf("[erasure_coding_write] Min write disk IO time = %0.10f\n", min_io);

    fclose(src_fp);

    free(buffer);
    free(data);
    free(coding);
    free(encode_metadata);
    free(network_metadata);
    return EC_OK;
}
static int erasure_coding_read(int argc, char **argv)
{
    printf("[erasure_coding_read] Read running...\n");
#if (SEND_DATANODE)
    int i;          // loop control variables
    int tmp_return; // return check

    /* File arguments */
    FILE *src_fp;        // src_file pointers
    int file_size;       // size of file
    int reading;         // number of reading required
    int current_reading; // number of current reading
    char src_filename[MAX_PATH_LEN] = {0};
    char dst_filename[MAX_PATH_LEN] = {0};
    char file_size_filename[MAX_PATH_LEN] = {0};
    char buf_filename1[MAX_PATH_LEN] = {0};
    char buf_filename2[MAX_PATH_LEN] = {0};
    getcwd(buf_filename1, sizeof(buf_filename1));
    strncpy(buf_filename2, buf_filename1, strlen(buf_filename1) - 6);
    sprintf(src_filename, "%s%s%s", buf_filename2, READ_PATH, argv[2]);            // get src_filename
    sprintf(dst_filename, "%s%s%s", buf_filename2, WRITE_PATH, argv[3]);           // get dst_filename
    sprintf(file_size_filename, "%s%s%s", buf_filename2, FILE_SIZE_PATH, argv[3]); // get file_size_filename to save file size of src file
    printf("[erasure_coding_read] The src_filename = %s\n", src_filename);
    printf("[erasure_coding_read] The dst_filename = %s\n", dst_filename);
    printf("[erasure_coding_read] The file_size_filename = %s\n", file_size_filename);

    /* EC arguments */
    int chunk_size = CHUNK_SIZE * 1024 * 1024;                 // unit Byte
    int buffer_size = EC_K * chunk_size;                       // stripe size
    char *buffer = (char *)malloc(sizeof(char) * buffer_size); // buffer for EC stripe
    if (buffer == NULL)
    {
        printf("[erasure_coding_read] Failed to malloc buffer: buffer_size = %d Bytes\n", buffer_size);
        return EC_ERROR;
    }

    /* Jerasure arguments */
    int *matrix = NULL;                                     // coding matrix
    char **data = (char **)malloc(sizeof(char *) * EC_K);   // data chunk
    char **coding = (char **)malloc(sizeof(char *) * EC_M); // coding chunk

    for (i = 0; i < EC_M; i++)
    {
        coding[i] = (char *)malloc(sizeof(char) * chunk_size);
        if (coding[i] == NULL)
        {
            printf("[erasure_coding_read] Failed to malloc coding: current coding_num = %d \n", i);
            return EC_ERROR;
        }
    }

    /* Set pointers to point to file data */
    for (i = 0; i < EC_K; i++)
    {
        data[i] = buffer + (i * chunk_size);
    }

    /* Timing variables */
    struct timeval t_dec1, t_dec2, t_net_io1, t_net_io2; // IO time for IO of datanode read chunk, not client write. If need IO time alone, need modify datanode_main.cpp
    struct timezone tz;
    double tsec;
    double totalsec_dec = 0.0, totalsec_net = 0.0;

    /* Open src_filename and error check, clean file and reopen */
    src_fp = fopen(src_filename, "wb");
    if (src_fp == NULL)
    {
        printf("[erasure_coding_read] Failed to wb open src file\n");
        return EC_ERROR;
    }
    fclose(src_fp);
    src_fp = fopen(src_filename, "ab");
    if (src_fp == NULL)
    {
        printf("[erasure_coding_read] Failed to ab open src file\n");
        return EC_ERROR;
    }

    /* Read file size from file_size filename */
    FILE *file_size_fp;
    char file_size_buffer[MAX_PATH_LEN] = {0};
    file_size_fp = fopen(file_size_filename, "rb");
    if (file_size_fp == NULL)
    {
        printf("[erasure_coding_read] Failed to open file_size file\n");
        fclose(src_fp);
        return EC_ERROR;
    }
    if (fread(file_size_buffer, sizeof(char), sizeof(file_size_buffer), file_size_fp) != sizeof(file_size_buffer))
    {
        printf("[erasure_coding_read] Failed to read file_size file\n");
        fclose(src_fp);
        fclose(file_size_fp);
        return EC_ERROR;
    }
    file_size = atoi(file_size_buffer);
    fclose(file_size_fp);

    /* Get remain chunks and remain size */
    int remain_chunks, remain_size;
    if (file_size % buffer_size == 0)
    {
        remain_chunks = EC_K;
        remain_size = 0;
    }
    else
    {
        remain_chunks = (file_size % buffer_size) / chunk_size;
        remain_size = (file_size % buffer_size) % chunk_size;
    }

    /* Create sockets , connect datanodes and set erasures */
    for (i = 0; i < EC_K + EC_M; i++)
    {
        erasures[i] = -1;
    }

    /* Create coding matrix */
    matrix = reed_sol_vandermonde_coding_matrix(EC_K, EC_M, EC_W);

    /* Number of reading */
    if (file_size % buffer_size == 0)
    {
        reading = file_size / buffer_size;
    }
    else
    {
        reading = file_size / buffer_size + 1;
    }
    current_reading = 1;

    int num_data_chunk;
    int num_need_coding;

    /* Begin EC */
    while (current_reading <= reading)
    {

        /* Create filename on stripe */
        char dst_filename_stripe[MAX_PATH_LEN] = {0};
        sprintf(dst_filename_stripe, "%s%d", dst_filename, current_reading);

        gettimeofday(&t_net_io1, &tz);

        /* Recv data chunk from each datanode */
        num_data_chunk = 0;
        tmp_return = recv_data_chunks_datanodes(&num_data_chunk, data, chunk_size, dst_filename_stripe);
        if (tmp_return == EC_ERROR)
        {
            printf("[erasure_coding_read] Failed to recv data chunks: current coding_num: current_reading = %d\n", current_reading);
            fclose(src_fp);
            return EC_ERROR;
        }

        /* If need decode, set decode flag */
        if (current_reading == 1)
        {
            num_need_coding = 0;
            for (i = 0; i < EC_K; i++)
            {
                if (erasures[i] != -1)
                {
                    num_need_coding++;
                }
            }
            if (num_need_coding > EC_M)
            {
                printf("[erasure_coding_read] Too many datanodes lost and cannot be recovered\n");
                fclose(src_fp);
                return EC_ERROR;
            }
        }

        /* Check if need decode */
        if (num_need_coding)
        {
            /* Recv coding chunk from each datanode */
            tmp_return = recv_coding_chunks_datanodes(&num_data_chunk, coding, num_need_coding, chunk_size, dst_filename_stripe);
            if (tmp_return == EC_ERROR)
            {
                printf("[erasure_coding_read] Failed to recv coding chunks: current coding_num: current_reading = %d\n", current_reading);
                fclose(src_fp);
                return EC_ERROR;
            }

            gettimeofday(&t_net_io2, &tz);
            tsec = 0.0;
            tsec += t_net_io2.tv_usec;
            tsec -= t_net_io1.tv_usec;
            tsec /= 1000000.0;
            tsec += t_net_io2.tv_sec;
            tsec -= t_net_io1.tv_sec;
            totalsec_net += tsec;
            printf("[erasure_coding_read] The %dth stripe: network time = %0.10f\n", current_reading, tsec);

            gettimeofday(&t_dec1, &tz);

            /* Decode */
            tmp_return = jerasure_matrix_decode(EC_K, EC_M, EC_W, matrix, 0, erasures, data, coding, chunk_size);
            if (tmp_return == EC_ERROR)
            {
                printf("[erasure_coding_read] Failed to jerasure_matrix_decode: current coding_num: current_reading = %d\n", current_reading);
                fclose(src_fp);
                return EC_ERROR;
            }

            gettimeofday(&t_dec2, &tz);
            tsec = 0.0;
            tsec += t_dec2.tv_usec;
            tsec -= t_dec1.tv_usec;
            tsec /= 1000000.0;
            tsec += t_dec2.tv_sec;
            tsec -= t_dec1.tv_sec;
            totalsec_dec += tsec;
            printf("[erasure_coding_read] The %dth stripe: decoding time = %0.10f\n", current_reading, tsec);
        }
        else
        {

            gettimeofday(&t_net_io2, &tz);
            tsec = 0.0;
            tsec += t_net_io2.tv_usec;
            tsec -= t_net_io1.tv_usec;
            tsec /= 1000000.0;
            tsec += t_net_io2.tv_sec;
            tsec -= t_net_io1.tv_sec;
            totalsec_net += tsec;
            printf("[erasure_coding_read] The %dth stripe: network and IO time = %0.10f\n", current_reading, tsec);
        }

        /* Check if final current_reading*/
        if (current_reading != reading)
        {
            for (i = 0; i < EC_K; i++)
            {
                if (fwrite(data[i], sizeof(char), (size_t)chunk_size, src_fp) != (size_t)chunk_size)
                {
                    printf("[handle_client] Failed to write buffer to src file: current_reading = %d\n", current_reading);
                    fclose(src_fp);
                    return EC_ERROR;
                }
            }
        }
        else
        {
            for (i = 0; i < remain_chunks; i++)
            {
                if (fwrite(data[i], sizeof(char), (size_t)chunk_size, src_fp) != (size_t)chunk_size)
                {
                    printf("[handle_client] Failed to write remain data chunks to src file: current_reading = %d\n", current_reading);
                    fclose(src_fp);
                    return EC_ERROR;
                }
            }
            if (fwrite(data[i], sizeof(char), (size_t)remain_size, src_fp) != (size_t)remain_size)
            {
                printf("[handle_client] Failed to write remain data size to src file: current_reading = %d\n", current_reading);
                fclose(src_fp);
                return EC_ERROR;
            }
        }
        current_reading++;
    }

    printf("[erasure_coding_read] The %d stripes: Average decoding time = %0.10f\n", reading, totalsec_dec / reading);
    printf("[erasure_coding_read] The %d stripes: Average network and IO time = %0.10f\n", reading, totalsec_net / reading);

    fclose(src_fp);

    return EC_OK;
#else
    printf("[erasure_coding_read] Network module not opened, because ych_ec_test.h: SEND_DATANODE is 0\n");
    return EC_OK;
#endif
}

/**
 * argv[1] cmd
 * argv[2] src_filename
 * argv[3] dst_filename
 */

int main(int argc, char *argv[])

{
    printf("[main] Client running...\n");
    /* error usage: print help information */
    if (argc < 2)
    {
        help(argc, argv);

        return EC_OK;
    }

    char cmd[16] = {0};
    strcpy(cmd, argv[1]);

    /* Cmd -h: help information */
    if (0 == strncmp(cmd, "-h", strlen("-h")))
    {
        help(argc, argv);
        return EC_OK;
    }

    /* Cmd -w: erasure coding write */
    else if (0 == strncmp(cmd, "-w", strlen("-w")))
    {
        if (erasure_coding_write(argc, argv) == EC_ERROR)
        {
            printf("[main] Failed to write\n");
        }
    }
    /* Cmd -wf: erasure coding write */
    else if (0 == strncmp(cmd, "-kw", strlen("-kw")))
    {
        if (erasure_coding_write_eck(argc, argv) == EC_ERROR)
        {
            printf("[main] Failed to write fast\n");
        }
    }
    /* Cmd -r: erasure coding read*/
    else if (0 == strncmp(cmd, "-r", strlen("-r")))
    {
        if (erasure_coding_read(argc, argv) == EC_ERROR)
        {
            printf("[main] Failed to read\n");
        }
    }
    else
    {
        printf("[main] Error operation, please read help\n");
        help(argc, argv);
    }

    return EC_OK;
}
