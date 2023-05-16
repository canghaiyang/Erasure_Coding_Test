#define TEST_N 10   // test times
#define ENC_THREAD_NUM 1// encoding thread num

#define EC_K 6       // k of k+m EC
#define EC_M 3      // m of k+m EC, larger than 1
#define EC_W 8       // finite field 2^w
#define CHUNK_SIZE 4 // unit MB
#define EC_X 3       // x number of encoded nodes
#define EC_N 3       // a chunk is divided into N blocks, larger than or equal to EC_X

#define SEND_DATANODE 1 // 1, send chunks to datanode; 0, just locally encode
#define SEND_METHOD 1   // 1, send in serial; 0,send in parallel
#define RECV_METHOD 1   // 1, recv in serial; 0,recv in parallel

#define WRITE_PATH "test_file/write/"                   // src_file and dst_file saved path
#define READ_PATH "test_file/read/"                     // src_file and dst_file saved path
#define FILE_SIZE_PATH "test_file/file_size/file_size_" // file_size file saved path
#define TEST_WRITE_IO_PATH "test_file/test_write_IO/"   // file_size file saved path
#define MAX_PATH_LEN 256                                // Max length of file path

#define IP_PREFIX "192.168.7."     // datanode ip prefix
#define DATANODE_START_IP_ADDR 102 // end_ip=start_ip+k+m, ip_addr_start (1-255,default ip_addr_end=ip_addr_start+k+m)
#define EC_WRITE_PORT 8000         // EC write network port
#define EC_READ_PORT 8001          // EC read network port
#define EC_WRITE_REQUEST_PORT 8002 // EC write request network port

#define EC_ERROR -1
#define EC_OK 0

typedef struct metadata_s // chunk metadata and data
{
    int sockfd;     // network socket fd
    int chunk_size; // chunk size
    int block_size; // chunk size
    int remain_block_size;
    int cur_block;
    int cur_eck;
    char *data;                               // chunk data or block data
    char dst_filename_datanode[MAX_PATH_LEN]; // dst filename on datanode
    int error_flag;                           // check if thread error
} metadata_t;

typedef struct encode_s // encode thread metadata
{
    int *matrix;   // coding matrix
    char **data;   // data chunk s
    char **coding; // coding chunks
    int chunk_size;
} encode_t;

typedef struct network_s // network thread metadata
{

    char **data;   // data chunk s
    char **coding; // coding chunks
    int chunk_size;
    char *dst_filename_stripe;
} network_t;
