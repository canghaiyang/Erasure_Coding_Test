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

char *buffer_chunk = nullptr;

int cur_block = 0;
pthread_mutex_t cond_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

int eck;

int sockfd_array[EC_X];

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
    close(metadata->sockfd);
    return nullptr;
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

void *handle_file_io(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;

    /* write chunk to disk */
    FILE *chunk_fp = fopen(metadata->dst_filename_datanode, "wb");
    if (chunk_fp == NULL)
    {
        printf("[handle_file_io] Failed to open dst_filename_datanode file\n");
        free(metadata->data);
        free(metadata);
        return nullptr;
    }
    if (fwrite(metadata->data, sizeof(char), (size_t)metadata->chunk_size, chunk_fp) != (size_t)metadata->chunk_size)
    {
        printf("[handle_file_io] Failed to write dst_filename_datanode file\n");
        fclose(chunk_fp);
        free(metadata->data);
        free(metadata);
        return nullptr;
    }
    fclose(chunk_fp);
    free(metadata->data);
    free(metadata);
    metadata = nullptr;
    return nullptr;
}
void *handle_client_write_new_ecx(void *arg)
{
    metadata_t *metadata = (metadata_t *)arg;
    int error_response = 0;

    pthread_mutex_lock(&cond_mutex);
    while (metadata->cur_block != cur_block)
    {
        pthread_cond_wait((pthread_cond_t *)&cond, &cond_mutex);
    }

    /* Send ecx metadata */
    metadata->sockfd = sockfd_array[metadata->cur_block % EC_X];
    if (send(metadata->sockfd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[handle_client_write_new_ecx] Failed to send block metadata to datanode\n");
        metadata->error_flag = EC_ERROR;
        free(metadata);
        return nullptr;
    }
    error_response = 0;
    if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_client_write_new_ecx] Failed to recv block metadata response from datanode\n");
        perror("recv"); // print error information
        metadata->error_flag = EC_ERROR;
        free(metadata);
        return nullptr;
    }
    if (error_response == 0)
    {
        printf("[handle_client_write_new_ecx] Failed to recv block metadata response from datanode\n");
        metadata->error_flag = EC_ERROR;
        free(metadata);
        return nullptr;
    }

    /* Send ecx data */
    char *buffer_block = nullptr;
    if (metadata->cur_block == 0)
    {
        buffer_block = buffer_chunk;
    }
    else
    {
        buffer_block = buffer_chunk + metadata->remain_block_size + metadata->cur_block * metadata->block_size;
    }
    metadata->data = buffer_block;
    if (send(metadata->sockfd, metadata->data, metadata->block_size, 0) < 0)
    {
        printf("[send_one_block_datanode] Failed to send block data to datanode\n");
        metadata->error_flag = EC_ERROR;
        free(metadata);
        return nullptr;
    }

    if (recv(metadata->sockfd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[send_one_block_datanode] Failed to recv block data response from datanode\n");
        metadata->error_flag = EC_ERROR;
        free(metadata);
        return nullptr;
    }
    if (error_response == 1)
    {
        printf("[send_one_block_datanode] Failed to recv block data response from datanode: recv error\n");
        metadata->error_flag = EC_ERROR;
        free(metadata);
        return nullptr;
    }

    cur_block = cur_block == EC_N - 1 ? 0 : cur_block + 1;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&cond_mutex);

    /* If cur_block is last block ,create thread to handle file IO */
    if (metadata->cur_block == EC_N - 1)
    {
        pthread_t tid;
        metadata->data = buffer_chunk;
        if (pthread_create(&tid, NULL, handle_file_io, (void *)metadata) != 0)
        {
            printf("[handle_client_write_new] Failed to create IO thread\n");
            free(metadata->data);
            free(metadata);
            return nullptr;
        }
        for (int i = 0; i < EC_X; i++)
        {
            close(sockfd_array[i]);
        }
    }
}

void *handle_client_write_new(void *arg)
{
    int client_fd = *((int *)arg);

    int i;
    int tmp_return;
    for (i = 0; i < EC_X; i++)
    {
        tmp_return = initialize_network(&sockfd_array[i], EC_WRITE_ECK_BASE_PORT + eck, EC_K + i);
        if (tmp_return == EC_ERROR)
        {
            printf("[handle_client_write_new] Failed to initialize network\n");
            close(client_fd);
            return nullptr;
        }
    }

    while (1)
    {
        metadata_t *metadata = (metadata_t *)malloc(sizeof(metadata_t));
        int error_response = 0;

        /* recv metadata and send response */
        if (recv(client_fd, metadata, sizeof(metadata_t), 0) < 0)
        {
            printf("[handle_client_write_new] Failed to recv metadata\n");
            free(metadata);
            close(client_fd);
            return nullptr;
        }
        error_response = 1;
        if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
        {
            printf("[handle_client_write_new] Failed to send metadata response to client\n");

            free(metadata);
            close(client_fd);
            return nullptr;
        }

        /* recv block data and send response */
        if (metadata->cur_eck > -1 && metadata->cur_eck < EC_K)
        {
#if (TEST_LOG)
            printf("[handle_client_write_new] cur_block = %d\n", metadata->cur_block);
#endif
            /* recv client datda */
            char *buffer_block = nullptr;
            if (metadata->cur_block == 0)
            {
                buffer_chunk = (char *)malloc(sizeof(char) * metadata->chunk_size);
                buffer_block = buffer_chunk;
            }
            else
            {
                buffer_block = buffer_chunk + metadata->remain_block_size + metadata->cur_block * metadata->block_size;
            }
            int recv_size;
            int tmp_block_size = metadata->block_size;
            char *tmp_buffer_block = buffer_block;
            while (tmp_block_size > 0)
            {
                recv_size = recv(client_fd, tmp_buffer_block, tmp_block_size, 0);
                if (recv_size < 0)
                {
                    printf("[handle_client_write_new] Failed to recv block data\n");
                    free(metadata);
                    free(buffer_chunk);
                    free(buffer_block);
                    close(client_fd);
                    return nullptr;
                }
                tmp_block_size -= recv_size;
                tmp_buffer_block += recv_size;
            }
            error_response = 0;
            if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
            {
                printf("[handle_client_write_new] Failed to send block data response to client\n");
                free(metadata);
                free(buffer_chunk);
                free(buffer_block);
                close(client_fd);
                return nullptr;
            }

            pthread_t tid_ecx;
            if (pthread_create(&tid_ecx, NULL, handle_client_write_new_ecx, (void *)metadata) != 0)
            {
                printf("[handle_client_write_new] Failed to create ecx thread\n");
                free(metadata->data);
                free(metadata);
                close(client_fd);
                return nullptr;
            }
            if (pthread_detach(tid_ecx) != 0)
            {
                printf("Failed to detach thread.\n");
                return nullptr;
            }
            if (metadata->cur_block == EC_N - 1)
            {
                break;
            }
        }
        else
        {
            printf("[handle_client_write_new] error recv: CUR_ECK = %d \n", metadata->cur_eck);
            return nullptr;
        }
    }

    close(client_fd);
    return nullptr;
}

void *handle_client_write(void *arg)
{
    int client_fd = *((int *)arg);

    metadata_t *metadata = (metadata_t *)malloc(sizeof(metadata_t));
    int error_response = 0;

    /* recv metadata and send response */
    if (recv(client_fd, metadata, sizeof(metadata_t), 0) < 0)
    {
        printf("[handle_client_write] Failed to recv metadata\n");
        free(metadata);
        return nullptr;
    }
    error_response = 1;
    if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
    {
        printf("[handle_client_write] Failed to send metadata response to client\n");
        free(metadata);
        return nullptr;
    }

    /* recv chunk data and send response */
    if (metadata->block_size == -1)
    {
        buffer_chunk = (char *)malloc(sizeof(char) * metadata->chunk_size); // buffer for EC chunk
        int recv_size;
        int tmp_chunk_size = metadata->chunk_size;
        char *tmp_buffer_chunk = buffer_chunk;
        while (tmp_chunk_size > 0)
        {
            recv_size = recv(client_fd, tmp_buffer_chunk, tmp_chunk_size, 0);
            if (recv_size < 0)
            {
                printf("[handle_client_write] Failed to recv chunc data\n");
                free(metadata);
                free(buffer_chunk);
                return nullptr;
            }
            tmp_chunk_size -= recv_size;
            tmp_buffer_chunk += recv_size;
        }
        error_response = 0;
        if (send(client_fd, &error_response, sizeof(error_response), 0) < 0)
        {
            printf("[handle_client_write] Failed to send chunk data response to client\n");
            free(metadata);
            free(buffer_chunk);
            return nullptr;
        }

        /* create thread to handle file IO */
        pthread_t tid;
        metadata->data = buffer_chunk;
        if (pthread_create(&tid, NULL, handle_file_io, (void *)metadata) != 0)
        {
            printf("[handle_client_write] Failed to create IO thread\n");
            free(metadata);
            free(buffer_chunk);
            return nullptr;
        }
    }
    else
    {
        printf("[handle_client_write] error recv: CUR_ECK = %d \n", metadata->cur_eck);
        return nullptr;
    }
    close(client_fd);
    return nullptr;
}

void *client_write_new(void *arg)
{
    printf("[client_write_new] Write new running\n");
    int tmp_return; // return check

    int datanode_fd; // datanode socket
    int client_fd;   // client socket

    /* initialize_network */
    tmp_return = server_initialize_network(&datanode_fd, EC_WRITE_NEW_PORT);
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
            printf("[client_write_new]  Failed to accept socket\n");
            close(datanode_fd);
            return nullptr;
        }

        /* create thread to handle client send */
        if (pthread_create(&tid, NULL, handle_client_write_new, (void *)&client_fd) != 0)
        {
            printf("[client_write_new]  Failed to create write thread\n");
            close(client_fd);
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

    /* initialize_network */
    tmp_return = server_initialize_network(&datanode_fd, EC_WRITE_PORT);
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
            close(datanode_fd);
            return nullptr;
        }

        /* create thread to handle client send */
        if (pthread_create(&tid, NULL, handle_client_write, (void *)&client_fd) != 0)
        {
            printf("[client_write] Failed to create write thread\n");
            close(client_fd);
            continue;
        }
    }
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
        free(metadata);
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
        free(metadata);
        return nullptr;
    }
    char *buffer_chunk = (char *)malloc(sizeof(char) * metadata->chunk_size); // buffer for EC chunk
    if (fread(buffer_chunk, sizeof(char), (size_t)metadata->chunk_size, chunk_fp) != (size_t)metadata->chunk_size)
    {
        printf("[handle_client_read] Failed to read chunk from file\n");
        fclose(chunk_fp);
        free(metadata);
        free(buffer_chunk);
        return nullptr;
    }
    fclose(chunk_fp);

    /* send chunk data to client*/
    if (send(client_fd, buffer_chunk, metadata->chunk_size, 0) < 0)
    {
        printf("[handle_client_read] Failed to send chunk data to client\n");
        free(metadata);
        free(buffer_chunk);
        return nullptr;
    }
    free(buffer_chunk);
    free(metadata);
    metadata = nullptr;
    return nullptr;
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
            close(client_fd);
            continue;
        }
    }

    close(datanode_fd);
}

int main()
{
    printf("[main] Datanode running...\n");

    pthread_t tid_write, tid_read, tid_write_new;

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

    int local_ip_last_num;
    int tmp_return;
    tmp_return = get_local_ip_lastnum(&local_ip_last_num);
    if (tmp_return == EC_ERROR)
    {
        printf("[main] Failed to get_local_ip_lastnum\n");
        return EC_ERROR;
    }
    eck = local_ip_last_num - DATANODE_START_IP_ADDR;

    /* create client_write_new thread to handle client write new */
    if (pthread_create(&tid_write_new, NULL, client_write_new, nullptr) != 0)
    {
        printf("[main] Failed to create client_write_new thread\n");
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
    if (pthread_join(tid_write_new, nullptr) != 0)
    {
        printf("[main] Failed to join client_write_new thread\n");
        return EC_ERROR;
    }

    return EC_OK;
}